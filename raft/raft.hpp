#pragma once

#include <vector>
#include <string>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <chrono>
#include <tuple>
#include "../common/labrpc.hpp"
#include "../common/channel.hpp"
#include "persister.hpp"

using namespace std;


//Raft - keeps a replicated log the same on every server, as long as a majority of them are up and
//can talk to each other. Follows Figure 2 of the paper (https://raft.github.io/raft.pdf) pretty
//much line for line, so that's the thing to have open when reading this.
//
//What the service on top sees:
//  start(command)   -> leader appends it to the log, returns (index, term, is_leader) right away
//  apply channel    -> once a command is committed, every server sends an ApplyMsg for it, in order
//  getState()       -> (term, am I the leader)


//What gets sent up to the service on the apply channel
struct ApplyMsg {
    bool command_valid = false;
    json command;
    int command_index = 0;
};


struct LogEntry {
    int term;
    json command;
};


enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
};


//Timing - heartbeats 10x a second, and the election timeout is a few heartbeats long so a live leader
//always gets a heartbeat out before anyone times out. Randomized so two followers rarely time out at
//the same moment and split the vote.
const int HEARTBEAT_MS = 100;
const int ELECTION_TIMEOUT_MIN_MS = 300;
const int ELECTION_TIMEOUT_MAX_MS = 600;


class Raft : public enable_shared_from_this<Raft> {

    public:

        //Use this instead of the constructor - the background threads need a shared_ptr to us,
        //which doesn't exist yet inside the constructor
        static shared_ptr<Raft> make(vector<shared_ptr<ClientEnd>> peers, int me,
                                     shared_ptr<Persister> persister, shared_ptr<Channel<ApplyMsg>> apply_ch){
            shared_ptr<Raft> rf(new Raft(peers, me, persister, apply_ch));
            rf->startThreads();
            return rf;
        }


        //Wraps our RPC handlers up as a Server for the network. Handlers hold a weak_ptr, so a
        //killed + dropped Raft just stops answering instead of crashing.
        shared_ptr<Server> makeServer(){
            auto server = make_shared<Server>();
            weak_ptr<Raft> weak = shared_from_this();

            server->addHandler("Raft.RequestVote", [weak](json args){
                auto rf = weak.lock();
                return rf ? rf->requestVote(args) : json();
            });
            server->addHandler("Raft.AppendEntries", [weak](json args){
                auto rf = weak.lock();
                return rf ? rf->appendEntries(args) : json();
            });
            return server;
        }


        pair<int, bool> getState(){
            lock_guard<mutex> lock(mu);
            return { current_term, state == LEADER };
        }


        //Leader only - stick a command on the end of the log. Returns right away, no promise it ever
        //commits (the leader could lose leadership before it replicates)
        tuple<int, int, bool> start(json command){
            lock_guard<mutex> lock(mu);
            if(state != LEADER || killed()){
                return { -1, current_term, false };
            }

            log.push_back({ current_term, command });
            match_index[me] = lastLogIndex();
            persist();

            //Send it out now instead of waiting for the next heartbeat, so commits are quick
            broadcastAppendEntries();
            return { lastLogIndex(), current_term, true };
        }


        void kill(){
            dead = true;
            apply_cond.notify_all();
        }


        bool killed(){
            return dead;
        }


    private:

        mutex mu;
        vector<shared_ptr<ClientEnd>> peers;
        int me;
        shared_ptr<Persister> persister;
        shared_ptr<Channel<ApplyMsg>> apply_ch;
        atomic<bool> dead{false};

        //Persistent state (Figure 2)
        int current_term = 0;
        int voted_for = -1;
        vector<LogEntry> log;      //log[0] is a dummy so real entries start at index 1

        //Volatile state
        RaftState state = FOLLOWER;
        chrono::steady_clock::time_point election_deadline;
        int commit_index = 0;      //highest index known to be committed
        int last_applied = 0;      //highest index sent up the apply channel
        condition_variable apply_cond;

        //Leader only - reset every time we win an election
        vector<int> next_index;    //next entry to send each peer
        vector<int> match_index;   //highest entry known to be replicated on each peer


        Raft(vector<shared_ptr<ClientEnd>> peers, int me, shared_ptr<Persister> persister,
             shared_ptr<Channel<ApplyMsg>> apply_ch){
            this->peers = peers;
            this->me = me;
            this->persister = persister;
            this->apply_ch = apply_ch;

            log.push_back({ 0, json() });
            next_index.resize(peers.size(), 1);
            match_index.resize(peers.size(), 0);
            resetElectionTimer();

            //Pick up wherever we were before a crash
            readPersist(persister->readRaftState());
        }


        void startThreads(){
            auto self = shared_from_this();
            thread([self](){ self->ticker(); }).detach();
            thread([self](){ self->applier(); }).detach();
        }


        // ---------- persistence ----------

        //Saves everything Figure 2 marks as persistent. Has to happen before we answer any RPC or
        //send anything that depends on it - e.g. if we vote, crash, and forget we voted, we could vote
        //twice in one term and get two leaders.
        //Caller holds mu
        void persist(){
            json state;
            state["current_term"] = current_term;
            state["voted_for"] = voted_for;
            state["log"] = json::array();
            for(LogEntry &entry : log){
                state["log"].push_back({ { "term", entry.term }, { "command", entry.command } });
            }
            persister->save(state.dump(), persister->readSnapshot());
        }


        void readPersist(string data){
            if(data.empty()){
                return;   //fresh start, nothing saved yet
            }

            json state = json::parse(data);
            current_term = state["current_term"];
            voted_for = state["voted_for"];
            log.clear();
            for(json &entry : state["log"]){
                log.push_back({ entry["term"], entry["command"] });
            }
        }


        // ---------- log helpers ----------

        int lastLogIndex(){
            return log.size() - 1;
        }

        int lastLogTerm(){
            return log.back().term;
        }

        int termAt(int index){
            return log[index].term;
        }


        //Last index in our log with this term, or -1 if we don't have any entries from it
        int lastIndexOfTerm(int term){
            for(int i = lastLogIndex(); i > 0; i --){
                if(termAt(i) == term){
                    return i;
                }
                if(termAt(i) < term){
                    break;
                }
            }
            return -1;
        }


        // ---------- elections ----------

        void resetElectionTimer(){
            int timeout = randInt(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS);
            election_deadline = chrono::steady_clock::now() + chrono::milliseconds(timeout);
        }


        //Anyone who sees a higher term in an RPC (request or reply) falls back to follower in that term
        //Caller holds mu
        void stepDown(int term){
            current_term = term;
            voted_for = -1;
            state = FOLLOWER;
            persist();
        }


        //Background loop - leaders send heartbeats, everyone else starts an election if they haven't
        //heard from a leader before their timer runs out
        void ticker(){
            auto last_heartbeat = chrono::steady_clock::now();

            while(!killed()){
                {
                    lock_guard<mutex> lock(mu);
                    auto now = chrono::steady_clock::now();

                    if(state == LEADER){
                        if(now - last_heartbeat >= chrono::milliseconds(HEARTBEAT_MS)){
                            broadcastAppendEntries();
                            last_heartbeat = now;
                        }
                    }
                    else if(now >= election_deadline){
                        startElection();
                    }
                }
                sleepMs(10);
            }
        }


        //Become a candidate, vote for ourselves, and ask everyone else for their vote in parallel
        //Caller holds mu
        void startElection(){
            state = CANDIDATE;
            current_term ++;
            voted_for = me;
            resetElectionTimer();
            persist();

            int term = current_term;
            auto votes = make_shared<int>(1);   //shared between the vote threads, guarded by mu

            json args;
            args["term"] = term;
            args["candidate_id"] = me;
            args["last_log_index"] = lastLogIndex();
            args["last_log_term"] = lastLogTerm();

            auto self = shared_from_this();
            for(int peer = 0; peer < peers.size(); peer ++){
                if(peer == me){
                    continue;
                }

                thread([self, peer, args, term, votes](){
                    json reply;
                    if(!self->peers[peer]->call("Raft.RequestVote", args, reply)){
                        return;
                    }

                    lock_guard<mutex> lock(self->mu);
                    int reply_term = reply["term"];

                    if(reply_term > self->current_term){
                        self->stepDown(reply_term);
                        return;
                    }

                    //Ignore stale votes from an older election, or if we already won/lost this one
                    if(self->state != CANDIDATE || self->current_term != term || !reply["vote_granted"]){
                        return;
                    }

                    (*votes) ++;
                    if(*votes > self->peers.size() / 2){
                        self->becomeLeader();
                    }
                }).detach();
            }
        }


        //Caller holds mu
        void becomeLeader(){
            state = LEADER;

            //Start by assuming everyone's log matches ours, the AppendEntries consistency check
            //walks next_index back for anyone who's behind
            for(int peer = 0; peer < peers.size(); peer ++){
                next_index[peer] = lastLogIndex() + 1;
                match_index[peer] = 0;
            }
            match_index[me] = lastLogIndex();

            broadcastAppendEntries();   //heartbeat right away so nobody else starts an election
        }


        //RPC handler: someone wants our vote
        json requestVote(json args){
            lock_guard<mutex> lock(mu);
            int term = args["term"];
            int candidate = args["candidate_id"];
            int their_last_index = args["last_log_index"];
            int their_last_term = args["last_log_term"];

            json reply;

            if(term > current_term){
                stepDown(term);
            }

            //Only vote for candidates whose log is at least as up to date as ours (5.4.1) - this is
            //what guarantees a new leader already has every committed entry
            bool up_to_date = their_last_term > lastLogTerm() ||
                              (their_last_term == lastLogTerm() && their_last_index >= lastLogIndex());

            bool grant = term == current_term && (voted_for == -1 || voted_for == candidate) && up_to_date;
            if(grant){
                voted_for = candidate;
                resetElectionTimer();   //only reset when we actually vote, or we could stall elections
                persist();
            }

            reply["term"] = current_term;
            reply["vote_granted"] = grant;
            return reply;
        }


        // ---------- log replication ----------

        //Caller holds mu
        void broadcastAppendEntries(){
            for(int peer = 0; peer < peers.size(); peer ++){
                if(peer != me){
                    sendAppendEntries(peer);
                }
            }
        }


        //Sends peer everything from next_index[peer] on (nothing = heartbeat), along with the entry
        //right before it so the follower can check its log matches ours up to that point
        //Caller holds mu
        void sendAppendEntries(int peer){
            int prev_index = next_index[peer] - 1;

            json args;
            args["term"] = current_term;
            args["leader_id"] = me;
            args["prev_log_index"] = prev_index;
            args["prev_log_term"] = termAt(prev_index);
            args["leader_commit"] = commit_index;
            args["entries"] = json::array();
            for(int i = prev_index + 1; i <= lastLogIndex(); i ++){
                args["entries"].push_back({ { "term", termAt(i) }, { "command", log[i].command } });
            }

            auto self = shared_from_this();
            thread([self, peer, args](){
                json reply;
                if(!self->peers[peer]->call("Raft.AppendEntries", args, reply)){
                    return;
                }

                lock_guard<mutex> lock(self->mu);
                self->handleAppendEntriesReply(peer, args, reply);
            }).detach();
        }


        //Caller holds mu
        void handleAppendEntriesReply(int peer, const json &args, const json &reply){
            int reply_term = reply["term"];
            if(reply_term > current_term){
                stepDown(reply_term);
                return;
            }

            //Stale reply from an old term, or we're not leader anymore
            if(state != LEADER || current_term != args["term"].get<int>()){
                return;
            }

            if(reply["success"]){
                //Use what we sent rather than our log now, since the log may have grown since
                int match = args["prev_log_index"].get<int>() + args["entries"].size();
                match_index[peer] = max(match_index[peer], match);
                next_index[peer] = max(next_index[peer], match_index[peer] + 1);
                advanceCommitIndex();
                return;
            }

            //Log didn't match - back next_index up. Instead of one entry at a time, the follower tells
            //us about the conflicting term so we can skip a whole term per round trip:
            //  x_term = -1        -> follower's log is too short, jump to its end (x_len)
            //  we have x_term     -> jump to just past our last entry in x_term
            //  we don't have it   -> jump to where the follower's x_term starts (x_index)
            int x_term = reply["x_term"];
            int x_index = reply["x_index"];
            int x_len = reply["x_len"];

            int next;
            if(x_term == -1){
                next = x_len;
            }
            else {
                int last = lastIndexOfTerm(x_term);
                next = last == -1 ? x_index : last + 1;
            }

            //Never go below what we know matches (a stale failure reply could ask us to)
            next_index[peer] = max(match_index[peer] + 1, min(next, lastLogIndex() + 1));

            //Try again right away instead of waiting a whole heartbeat
            sendAppendEntries(peer);
        }


        //An entry is committed once a majority has it - but only count entries from our own term
        //(Figure 8 in the paper shows how committing an older term's entry by counting can go wrong)
        //Caller holds mu
        void advanceCommitIndex(){
            for(int n = lastLogIndex(); n > commit_index; n --){
                if(termAt(n) != current_term){
                    break;   //terms only go down as we walk back, so nothing earlier is ours either
                }

                int count = 0;
                for(int peer = 0; peer < peers.size(); peer ++){
                    if(match_index[peer] >= n){
                        count ++;
                    }
                }

                if(count > peers.size() / 2){
                    commit_index = n;
                    apply_cond.notify_all();
                    return;
                }
            }
        }


        //RPC handler: entries (or a heartbeat) from the leader
        json appendEntries(json args){
            lock_guard<mutex> lock(mu);
            int term = args["term"];
            json reply;

            if(term < current_term){
                reply["term"] = current_term;
                reply["success"] = false;
                reply["x_term"] = -1;
                reply["x_index"] = -1;
                reply["x_len"] = lastLogIndex() + 1;
                return reply;
            }

            //Valid leader for this term - if we were a candidate in the same term, it won, so give up
            if(term > current_term){
                stepDown(term);
            }
            state = FOLLOWER;
            resetElectionTimer();

            int prev_index = args["prev_log_index"];
            int prev_term = args["prev_log_term"];
            reply["term"] = current_term;
            reply["success"] = false;
            reply["x_len"] = lastLogIndex() + 1;

            //Our log is too short to have prev_index at all
            if(prev_index > lastLogIndex()){
                reply["x_term"] = -1;
                reply["x_index"] = -1;
                return reply;
            }

            //We have prev_index but from a different term - report that term + where it starts
            if(termAt(prev_index) != prev_term){
                int x_term = termAt(prev_index);
                int x_index = prev_index;
                while(x_index - 1 > 0 && termAt(x_index - 1) == x_term){
                    x_index --;
                }
                reply["x_term"] = x_term;
                reply["x_index"] = x_index;
                return reply;
            }

            //Logs match up to prev_index. Add the new entries, but only cut our log at an actual
            //conflict - an old, delayed AppendEntries must not erase entries a newer one already added
            int index = prev_index;
            bool changed = false;
            for(const json &entry : args["entries"]){
                index ++;
                int entry_term = entry["term"];

                if(index <= lastLogIndex()){
                    if(termAt(index) == entry_term){
                        continue;
                    }
                    log.resize(index);   //conflict - drop this entry and everything after
                }
                log.push_back({ entry_term, entry["command"] });
                changed = true;
            }
            if(changed){
                persist();
            }

            //index is now the last entry this RPC covered - we can only commit up to there, since
            //anything past it in our log isn't known to match the leader
            int leader_commit = args["leader_commit"];
            if(leader_commit > commit_index){
                commit_index = min(leader_commit, index);
                apply_cond.notify_all();
            }

            reply["success"] = true;
            return reply;
        }


        // ---------- applying ----------

        //Background loop - sends newly committed entries up the apply channel, in order.
        //The channel push happens without holding mu so the service can call back into us freely.
        void applier(){
            unique_lock<mutex> lock(mu);

            while(!killed()){
                if(last_applied < commit_index){
                    vector<ApplyMsg> msgs;
                    for(int i = last_applied + 1; i <= commit_index; i ++){
                        ApplyMsg msg;
                        msg.command_valid = true;
                        msg.command = log[i].command;
                        msg.command_index = i;
                        msgs.push_back(msg);
                    }
                    last_applied = commit_index;

                    lock.unlock();
                    for(ApplyMsg &msg : msgs){
                        apply_ch->push(msg);
                    }
                    lock.lock();
                }
                else {
                    //Timed wait so a kill() that sneaks in between the check and the wait can't hang us
                    apply_cond.wait_for(lock, chrono::milliseconds(100));
                }
            }
        }

};
