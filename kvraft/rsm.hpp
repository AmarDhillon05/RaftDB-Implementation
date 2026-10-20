#pragma once

#include <vector>
#include <string>
#include <map>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include "../common/labrpc.hpp"
#include "../common/channel.hpp"
#include "../common/kv.hpp"
#include "../raft/raft.hpp"

using namespace std;


//Replicated state machine - the glue between raft and a service (the kv server, or the shard groups
//in lab 5). The service just implements StateMachine and calls submit(); this layer handles getting
//the request through raft and handing back the result once it's actually been applied.
//
//  submit(req)   -> puts req in the raft log, waits for it to come out of the apply channel, runs it
//                   through the state machine, returns the result. ErrWrongLeader if we're not the
//                   leader or lose leadership before it commits.
//
//Every server runs every committed request through its own state machine in log order, so they all
//end up in the same state.


//What a service has to provide
class StateMachine {
    public:
        virtual json doOp(json req) = 0;           //apply one request, return its result
        virtual string snapshot() = 0;             //all state, serialized
        virtual void restore(string data) = 0;     //replace all state from a snapshot
        virtual ~StateMachine(){}
};


class RSM {

    public:

        //max_raft_state = -1 means never snapshot
        RSM(vector<shared_ptr<ClientEnd>> peers, int me, shared_ptr<Persister> persister, int max_raft_state,
            StateMachine* sm){
            this->me = me;
            this->max_raft_state = max_raft_state;
            this->sm = sm;
            this->persister = persister;

            //Random starting id, so ops from a restarted server never look like ops from its old self
            next_id = ((int64_t) randInt(0, 1 << 30) << 30) | randInt(0, 1 << 30);

            //Coming back from a crash - load the last snapshot before raft starts replaying the log after it
            string snap = persister->readSnapshot();
            if(!snap.empty()){
                sm->restore(snap);
            }

            apply_ch = make_shared<Channel<ApplyMsg>>();
            rf = Raft::make(peers, me, persister, apply_ch);
            reader = thread([this](){ readApplyCh(); });
        }


        ~RSM(){
            kill();
        }


        shared_ptr<Raft> raft(){
            return rf;
        }


        //Returns (OK, result) once req has been committed and applied, or (ErrWrongLeader, null)
        pair<string, json> submit(json req){
            unique_lock<mutex> lock(mu);

            json op;
            op["me"] = me;
            op["id"] = next_id ++;
            op["req"] = req;

            //Hold mu across start() + registering as a waiter - otherwise the entry could commit and get
            //applied in between, and nobody would ever wake us up. (Safe since raft never calls back into
            //us, so the lock order is always rsm -> raft.)
            auto [index, term, is_leader] = rf->start(op);
            if(!is_leader){
                return { ErrWrongLeader, json() };
            }

            auto pending = make_shared<Pending>();
            pending->id = op["id"];
            waiting[index] = pending;

            //Wait for whatever ends up at index to get applied. If we lose leadership (or the term
            //changes), some other entry might end up there instead and we could wait forever, so give up.
            while(!pending->done){
                pending->cond.wait_for(lock, chrono::milliseconds(50));
                if(pending->done){
                    break;
                }

                auto [cur_term, still_leader] = rf->getState();
                if(dead || cur_term != term || !still_leader){
                    if(waiting.count(index) && waiting[index] == pending){
                        waiting.erase(index);
                    }
                    return { ErrWrongLeader, json() };
                }
            }

            //The entry at index has to actually be ours - a new leader could have put something else there
            if(!pending->ours){
                return { ErrWrongLeader, json() };
            }
            return { OK, pending->result };
        }


        void kill(){
            if(dead.exchange(true)){
                return;
            }
            rf->kill();
            apply_ch->close();
            reader.join();
        }


    private:

        //A submit() waiting for its index to get applied
        struct Pending {
            int64_t id;
            bool done = false;
            bool ours = false;
            json result;
            condition_variable cond;
        };

        mutex mu;
        int me;
        int max_raft_state;
        StateMachine* sm;
        shared_ptr<Persister> persister;
        shared_ptr<Raft> rf;
        shared_ptr<Channel<ApplyMsg>> apply_ch;
        thread reader;
        atomic<bool> dead{false};
        int64_t next_id;
        map<int, shared_ptr<Pending>> waiting;     //raft index -> who's waiting on it


        //Background loop - runs every committed op through the state machine, in order, and wakes up
        //whoever submitted it (if they're on this server)
        void readApplyCh(){
            while(true){
                optional<ApplyMsg> msg = apply_ch->pop();
                if(!msg){
                    return;   //closed by kill()
                }

                if(msg->snapshot_valid){
                    //We fell behind and the leader sent its snapshot - replace our state with it
                    lock_guard<mutex> lock(mu);
                    sm->restore(msg->snapshot);
                    continue;
                }
                if(!msg->command_valid){
                    continue;
                }

                json op = msg->command;
                int index = msg->command_index;

                lock_guard<mutex> lock(mu);
                json result = sm->doOp(op["req"]);

                if(waiting.count(index)){
                    auto pending = waiting[index];
                    waiting.erase(index);
                    pending->ours = op["me"].get<int>() == me && op["id"].get<int64_t>() == pending->id;
                    pending->result = result;
                    pending->done = true;
                    pending->cond.notify_all();
                }

                //Log getting too big - snapshot everything through this index so raft can trim it
                if(max_raft_state != -1 && rf->persistedSize() >= max_raft_state){
                    rf->snapshot(index, sm->snapshot());
                }
            }
        }

};
