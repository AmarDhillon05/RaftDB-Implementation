#pragma once

#include <vector>
#include <string>
#include <map>
#include <set>
#include <memory>
#include <mutex>
#include <thread>
#include <chrono>
#include <iostream>
#include <iomanip>
#include "../../common/testing.hpp"
#include "../../common/labrpc.hpp"
#include "../raft.hpp"

using namespace std;


//Test harness for raft (port of config.go from the MIT lab). Runs n Rafts on a fake network and gives
//tests ways to break things - disconnect, crash, restart, make the network unreliable - plus checks
//that every server applies the same command at the same index.


struct RaftTest {

    int n;
    shared_ptr<Network> net = make_shared<Network>();
    vector<shared_ptr<Raft>> rafts;
    vector<bool> connected;
    vector<shared_ptr<Persister>> saved;
    vector<vector<string>> endnames;           //endnames[i][j] = the end server i uses to call j
    vector<shared_ptr<Channel<ApplyMsg>>> apply_chs;
    vector<thread> appliers;

    mutex mu;
    vector<map<int, json>> logs;               //what each server has applied, index -> command
    vector<int> last_applied;
    int max_index = 0;

    chrono::steady_clock::time_point start_time;
    int rpcs0 = 0;
    long long bytes0 = 0;
    int max_index0 = 0;


    RaftTest(int n, bool reliable){
        this->n = n;
        net->setReliable(reliable);
        net->setLongDelays(true);

        rafts.resize(n);
        connected.resize(n, false);
        saved.resize(n);
        endnames.resize(n, vector<string>(n));
        apply_chs.resize(n);
        appliers.resize(n);
        logs.resize(n);
        last_applied.resize(n, 0);

        for(int i = 0; i < n; i ++){
            start1(i);
        }
        for(int i = 0; i < n; i ++){
            connect(i);
        }
    }


    ~RaftTest(){
        for(int i = 0; i < n; i ++){
            crash1(i);
        }
    }


    // ---------- applying + checking ----------

    //Every committed command gets checked against what every other server applied at that index
    void applier(int i, shared_ptr<Channel<ApplyMsg>> ch){
        while(true){
            optional<ApplyMsg> msg = ch->pop();
            if(!msg){
                return;
            }
            if(!msg->command_valid){
                continue;
            }

            lock_guard<mutex> lock(mu);
            int index = msg->command_index;

            for(int j = 0; j < n; j ++){
                if(logs[j].count(index) && logs[j][index] != msg->command){
                    check(false, "commit index=" + to_string(index) + " server=" + to_string(i) + " " +
                          msg->command.dump() + " != server=" + to_string(j) + " " + logs[j][index].dump());
                }
            }
            check(index == last_applied[i] + 1, "server " + to_string(i) + " apply out of order " +
                  to_string(index) + " after " + to_string(last_applied[i]));

            logs[i][index] = msg->command;
            last_applied[i] = index;
            max_index = max(max_index, index);
        }
    }


    // ---------- starting / crashing servers ----------

    //Kill server i, but keep its persisted state around so start1 can bring it back
    void crash1(int i){
        disconnect(i);
        net->deleteServer(to_string(i));

        shared_ptr<Raft> rf;
        {
            lock_guard<mutex> lock(mu);
            rf = rafts[i];
            rafts[i] = nullptr;
        }
        if(rf){
            rf->kill();
        }

        //Copy so anything the dead instance still writes doesn't leak into the next one's "disk"
        if(saved[i]){
            saved[i] = saved[i]->copy();
        }

        if(apply_chs[i]){
            apply_chs[i]->close();
            appliers[i].join();
            apply_chs[i] = nullptr;
        }
    }


    //(Re)start server i from whatever it had persisted
    void start1(int i){
        crash1(i);

        //Fresh ends so old in-flight calls from the dead instance can't be confused with new ones
        vector<shared_ptr<ClientEnd>> ends;
        for(int j = 0; j < n; j ++){
            endnames[i][j] = "end-" + to_string(i) + "-" + to_string(j) + "-" + to_string(randInt(0, 1 << 30));
            ends.push_back(net->makeEnd(endnames[i][j]));
            net->connect(endnames[i][j], to_string(j));
        }

        saved[i] = saved[i] ? saved[i]->copy() : make_shared<Persister>();

        {
            lock_guard<mutex> lock(mu);
            last_applied[i] = 0;
        }

        apply_chs[i] = make_shared<Channel<ApplyMsg>>();
        auto ch = apply_chs[i];
        appliers[i] = thread([this, i, ch](){ applier(i, ch); });

        auto rf = Raft::make(ends, i, saved[i], apply_chs[i]);
        {
            lock_guard<mutex> lock(mu);
            rafts[i] = rf;
        }
        net->addServer(to_string(i), rf->makeServer());
    }


    //rafts[] changes under crash1/start1, so anything running on another thread should read it through here
    shared_ptr<Raft> getRaft(int i){
        lock_guard<mutex> lock(mu);
        return rafts[i];
    }


    // ---------- network ----------

    void connect(int i){
        connected[i] = true;
        for(int j = 0; j < n; j ++){
            if(connected[j]){
                net->enable(endnames[i][j], true);
                net->enable(endnames[j][i], true);
            }
        }
    }


    void disconnect(int i){
        connected[i] = false;
        for(int j = 0; j < n; j ++){
            if(!endnames[i][j].empty()){
                net->enable(endnames[i][j], false);
            }
            if(!endnames[j][i].empty()){
                net->enable(endnames[j][i], false);
            }
        }
    }


    void setUnreliable(bool unrel){
        net->setReliable(!unrel);
    }


    void setLongReordering(bool on){
        net->setLongReordering(on);
    }


    int rpcCount(int server){
        return net->getCount(to_string(server));
    }


    int rpcTotal(){
        return net->getTotalCount();
    }


    long long bytesTotal(){
        return net->getTotalBytes();
    }


    // ---------- checks ----------

    //Makes sure exactly one connected server thinks it's leader (per term), retrying for a while
    //since an election might be in progress. Returns the leader.
    int checkOneLeader(){
        for(int iters = 0; iters < 10; iters ++){
            sleepMs(450 + randInt(0, 100));

            map<int, vector<int>> leaders;   //term -> leaders
            for(int i = 0; i < n; i ++){
                if(connected[i]){
                    auto [term, is_leader] = rafts[i]->getState();
                    if(is_leader){
                        leaders[term].push_back(i);
                    }
                }
            }

            int last_term = -1;
            for(auto &[term, ls] : leaders){
                check(ls.size() <= 1, "term " + to_string(term) + " has " + to_string(ls.size()) + " (>1) leaders");
                last_term = max(last_term, term);
            }

            if(!leaders.empty()){
                return leaders[last_term][0];
            }
        }
        check(false, "expected one leader, got none");
        return -1;
    }


    //Every connected server should agree on the term
    int checkTerms(){
        int term = -1;
        for(int i = 0; i < n; i ++){
            if(connected[i]){
                int t = rafts[i]->getState().first;
                if(term == -1){
                    term = t;
                }
                else {
                    check(term == t, "servers disagree on term");
                }
            }
        }
        return term;
    }


    void checkNoLeader(){
        for(int i = 0; i < n; i ++){
            if(connected[i]){
                check(!rafts[i]->getState().second, "expected no leader among connected servers, but " +
                      to_string(i) + " claims to be leader");
            }
        }
    }


    //How many servers think index is committed (and checks they all agree on the command)
    pair<int, json> nCommitted(int index){
        lock_guard<mutex> lock(mu);
        int count = 0;
        json cmd;

        for(int i = 0; i < n; i ++){
            if(logs[i].count(index)){
                if(count > 0){
                    check(cmd == logs[i][index], "committed values do not match: index " + to_string(index));
                }
                count ++;
                cmd = logs[i][index];
            }
        }
        return { count, cmd };
    }


    //Wait for index to commit on at least n servers - gives up early if the term moves on (since then
    //the entry might never commit). Returns the command, or -1 if we gave up.
    json wait(int index, int n_servers, int start_term){
        int to = 10;
        for(int iters = 0; iters < 30; iters ++){
            auto [count, cmd] = nCommitted(index);
            if(count >= n_servers){
                break;
            }
            sleepMs(to);
            if(to < 1000){
                to *= 2;
            }

            if(start_term > -1){
                for(int i = 0; i < n; i ++){
                    auto rf = getRaft(i);
                    if(rf && rf->getState().first > start_term){
                        return -1;
                    }
                }
            }
        }

        auto [count, cmd] = nCommitted(index);
        check(count >= n_servers, "only " + to_string(count) + " decided for index " + to_string(index) +
              "; wanted " + to_string(n_servers));
        return cmd;
    }


    //The main "does it work" helper - keeps finding a leader and submitting cmd until it shows up as
    //committed on expected_servers servers. With retry=false, one leader gets one shot.
    //Returns the index it committed at.
    int one(json cmd, int expected_servers, bool retry){
        auto t0 = chrono::steady_clock::now();
        int starts = 0;

        while(chrono::steady_clock::now() - t0 < chrono::seconds(10)){

            //Try every server until one says it's leader
            int index = -1;
            for(int si = 0; si < n; si ++){
                starts = (starts + 1) % n;
                shared_ptr<Raft> rf;
                if(connected[starts]){
                    rf = getRaft(starts);
                }
                if(rf){
                    auto [idx, term, ok] = rf->start(cmd);
                    if(ok){
                        index = idx;
                        break;
                    }
                }
            }

            if(index != -1){
                //Someone took it - give it ~2s to commit
                auto t1 = chrono::steady_clock::now();
                while(chrono::steady_clock::now() - t1 < chrono::seconds(2)){
                    auto [count, cmd1] = nCommitted(index);
                    if(count > 0 && count >= expected_servers && cmd1 == cmd){
                        return index;
                    }
                    sleepMs(20);
                }
                check(retry, "one(" + cmd.dump() + ") failed to reach agreement");
            }
            else {
                sleepMs(50);
            }
        }
        check(false, "one(" + cmd.dump() + ") failed to reach agreement");
        return -1;
    }


    // ---------- reporting ----------

    void begin(string description){
        cout << description << " ..." << endl;
        start_time = chrono::steady_clock::now();
        rpcs0 = rpcTotal();
        bytes0 = bytesTotal();
        lock_guard<mutex> lock(mu);
        max_index0 = max_index;
    }


    //Prints time, peers, rpcs, bytes, commits the same way the Go tests do
    void end(){
        double secs = chrono::duration<double>(chrono::steady_clock::now() - start_time).count();
        int n_rpc = rpcTotal() - rpcs0;
        long long n_bytes = bytesTotal() - bytes0;
        int n_cmds;
        {
            lock_guard<mutex> lock(mu);
            n_cmds = max_index - max_index0;
        }
        cout << "  ... Passed --" << fixed << setprecision(1) << setw(5) << secs << "  " << n << " "
             << setw(5) << n_rpc << " " << setw(8) << n_bytes << " " << setw(4) << n_cmds << endl;
    }

};
