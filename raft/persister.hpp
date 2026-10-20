#pragma once

#include <string>
#include <mutex>
#include <memory>

using namespace std;


//Stand-in for disk. Raft saves its state here, and when the tests "crash" a server, the new Raft
//instance gets the same persister back so it can recover from what was saved.
//Raft state + snapshot are always saved together, so a crash can never leave one without the other.
class Persister {

    public:

        //Tests copy the persister on crash, so the dead instance can't keep writing to the new one's "disk"
        shared_ptr<Persister> copy(){
            lock_guard<mutex> lock(mu);
            auto p = make_shared<Persister>();
            p->raft_state = raft_state;
            p->snapshot = snapshot;
            return p;
        }


        void save(string state, string snap){
            lock_guard<mutex> lock(mu);
            raft_state = state;
            snapshot = snap;
        }


        string readRaftState(){
            lock_guard<mutex> lock(mu);
            return raft_state;
        }


        int raftStateSize(){
            lock_guard<mutex> lock(mu);
            return raft_state.size();
        }


        string readSnapshot(){
            lock_guard<mutex> lock(mu);
            return snapshot;
        }


        int snapshotSize(){
            lock_guard<mutex> lock(mu);
            return snapshot.size();
        }


    private:

        mutex mu;
        string raft_state;
        string snapshot;

};
