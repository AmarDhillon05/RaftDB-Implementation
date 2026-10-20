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
            return { -1, current_term, false };
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
        condition_variable apply_cond;


        Raft(vector<shared_ptr<ClientEnd>> peers, int me, shared_ptr<Persister> persister,
             shared_ptr<Channel<ApplyMsg>> apply_ch){
            this->peers = peers;
            this->me = me;
            this->persister = persister;
            this->apply_ch = apply_ch;

            log.push_back({ 0, json() });
            resetElectionTimer();
        }


        void startThreads(){
            auto self = shared_from_this();
            thread([self](){ self->ticker(); }).detach();
        }


        // ---------- log helpers ----------

        int lastLogIndex(){
            return log.size() - 1;
        }

        int lastLogTerm(){
            return log.back().term;
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
            }

            reply["term"] = current_term;
            reply["vote_granted"] = grant;
            return reply;
        }


        // ---------- log replication (just heartbeats for now) ----------

        //Caller holds mu
        void broadcastAppendEntries(){
            json args;
            args["term"] = current_term;
            args["leader_id"] = me;

            auto self = shared_from_this();
            for(int peer = 0; peer < peers.size(); peer ++){
                if(peer == me){
                    continue;
                }

                thread([self, peer, args](){
                    json reply;
                    if(!self->peers[peer]->call("Raft.AppendEntries", args, reply)){
                        return;
                    }

                    lock_guard<mutex> lock(self->mu);
                    int reply_term = reply["term"];
                    if(reply_term > self->current_term){
                        self->stepDown(reply_term);
                    }
                }).detach();
            }
        }


        //RPC handler: heartbeat from the leader
        json appendEntries(json args){
            lock_guard<mutex> lock(mu);
            int term = args["term"];
            json reply;

            if(term < current_term){
                reply["term"] = current_term;
                reply["success"] = false;
                return reply;
            }

            //Valid leader for this term - if we were a candidate in the same term, it won, so give up
            if(term > current_term){
                stepDown(term);
            }
            state = FOLLOWER;
            resetElectionTimer();

            reply["term"] = current_term;
            reply["success"] = true;
            return reply;
        }

};
