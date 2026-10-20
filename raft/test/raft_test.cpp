#include <vector>
#include <thread>
#include <atomic>
#include "config.hpp"

using namespace std;

//Raft tests - ported from test_test.go in the MIT lab, same names so they're easy to compare


const int RAFT_ELECTION_TIMEOUT_MS = 1000;


// ---------- 3A: leader election ----------

void testInitialElection3A(){
    RaftTest t(3, true);
    t.begin("Test (3A): initial election");

    //Is a leader elected?
    t.checkOneLeader();

    //Sleep a bit to avoid racing with followers learning of the election, then check all peers agree
    sleepMs(50);
    int term1 = t.checkTerms();
    check(term1 >= 1, "term is " + to_string(term1) + ", but should be at least 1");

    //Does the leader + term stay the same if there is no network failure?
    sleepMs(2 * RAFT_ELECTION_TIMEOUT_MS);
    int term2 = t.checkTerms();
    if(term1 != term2){
        cout << "warning: term changed even though there were no failures" << endl;
    }

    //There should still be a leader
    t.checkOneLeader();
    t.end();
}


void testReElection3A(){
    int servers = 3;
    RaftTest t(servers, true);
    t.begin("Test (3A): election after network failure");

    int leader1 = t.checkOneLeader();

    //If the leader disconnects, a new one should be elected
    t.disconnect(leader1);
    t.checkOneLeader();

    //If the old leader rejoins, that shouldn't disturb the new leader. And the old leader should
    //switch to follower.
    t.connect(leader1);
    int leader2 = t.checkOneLeader();

    //If there's no quorum, no new leader should be elected
    t.disconnect(leader2);
    t.disconnect((leader2 + 1) % servers);
    sleepMs(2 * RAFT_ELECTION_TIMEOUT_MS);

    //Check that the one connected server does not think it is the leader
    t.checkNoLeader();

    //If a quorum arises, it should elect a leader
    t.connect((leader2 + 1) % servers);
    t.checkOneLeader();

    //Re-join of last node shouldn't prevent leader from existing
    t.connect(leader2);
    t.checkOneLeader();
    t.end();
}


void testManyElections3A(){
    int servers = 7;
    RaftTest t(servers, true);
    t.begin("Test (3A): multiple elections");

    t.checkOneLeader();

    for(int iter = 0; iter < 10; iter ++){
        //Disconnect three nodes
        int i1 = randInt(0, servers);
        int i2 = randInt(0, servers);
        int i3 = randInt(0, servers);
        t.disconnect(i1);
        t.disconnect(i2);
        t.disconnect(i3);

        //Either the current leader should still be alive, or the remaining four should elect a new one
        t.checkOneLeader();

        t.connect(i1);
        t.connect(i2);
        t.connect(i3);
    }

    t.checkOneLeader();
    t.end();
}


// ---------- 3B: log replication ----------

//Helper to make a random string command (so commands are unique)
string randString(int n){
    string chars = "abcdefghijklmnopqrstuvwxyz0123456789";
    string out;
    for(int i = 0; i < n; i ++){
        out += chars[randInt(0, chars.size())];
    }
    return out;
}


void testBasicAgree3B(){
    int servers = 3;
    RaftTest t(servers, true);
    t.begin("Test (3B): basic agreement");

    for(int index = 1; index < 4; index ++){
        auto [nd, cmd] = t.nCommitted(index);
        check(nd == 0, "some have committed before start()");

        int xindex = t.one(index * 100, servers, false);
        check(xindex == index, "got index " + to_string(xindex) + " but expected " + to_string(index));
    }
    t.end();
}


//Each command should only be sent to each peer once (roughly)
void testRPCBytes3B(){
    int servers = 3;
    RaftTest t(servers, true);
    t.begin("Test (3B): RPC byte count");

    t.one(99, servers, false);
    long long bytes0 = t.bytesTotal();

    long long sent = 0;
    for(int index = 2; index < 10; index ++){
        string cmd = randString(5000);
        int xindex = t.one(cmd, servers, false);
        check(xindex == index, "got index " + to_string(xindex) + " but expected " + to_string(index));
        sent += cmd.size();
    }

    long long got = t.bytesTotal() - bytes0;
    long long expected = servers * sent;
    check(got <= expected + 50000, "too many RPC bytes; got " + to_string(got) + ", expected " + to_string(expected));
    t.end();
}


//Test just failure of followers
void testFollowerFailure3B(){
    int servers = 3;
    RaftTest t(servers, true);
    t.begin("Test (3B): test progressive failure of followers");

    t.one(101, servers, false);

    //Disconnect one follower from the network
    int leader1 = t.checkOneLeader();
    t.disconnect((leader1 + 1) % servers);

    //The leader and remaining follower should be able to agree despite the disconnected follower
    t.one(102, servers - 1, false);
    sleepMs(RAFT_ELECTION_TIMEOUT_MS);
    t.one(103, servers - 1, false);

    //Disconnect the remaining follower
    int leader2 = t.checkOneLeader();
    t.disconnect((leader2 + 1) % servers);
    t.disconnect((leader2 + 2) % servers);

    //Submit a command
    auto [index, term, ok] = t.rafts[leader2]->start(104);
    check(ok, "leader rejected start()");
    check(index == 4, "expected index 4, got " + to_string(index));

    sleepMs(2 * RAFT_ELECTION_TIMEOUT_MS);

    //Check that command 104 did not commit
    check(t.nCommitted(index).first == 0, "command committed without majority");
    t.end();
}


//Test just failure of leaders
void testLeaderFailure3B(){
    int servers = 3;
    RaftTest t(servers, true);
    t.begin("Test (3B): test failure of leaders");

    t.one(101, servers, false);

    //Disconnect the first leader
    int leader1 = t.checkOneLeader();
    t.disconnect(leader1);

    //The remaining followers should elect a new leader
    t.one(102, servers - 1, false);
    sleepMs(RAFT_ELECTION_TIMEOUT_MS);
    t.one(103, servers - 1, false);

    //Disconnect the new leader
    int leader2 = t.checkOneLeader();
    t.disconnect(leader2);

    //Submit a command to each server
    for(int i = 0; i < servers; i ++){
        t.rafts[i]->start(104);
    }

    sleepMs(2 * RAFT_ELECTION_TIMEOUT_MS);

    //Check that command 104 did not commit
    check(t.nCommitted(4).first == 0, "command committed without majority");
    t.end();
}


//A follower that was disconnected should catch up when it comes back
void testFailAgree3B(){
    int servers = 3;
    RaftTest t(servers, true);
    t.begin("Test (3B): agreement after follower reconnects");

    t.one(101, servers, false);

    int leader = t.checkOneLeader();
    t.disconnect((leader + 1) % servers);

    //The leader and remaining follower should be able to agree
    t.one(102, servers - 1, false);
    t.one(103, servers - 1, false);
    sleepMs(RAFT_ELECTION_TIMEOUT_MS);
    t.one(104, servers - 1, false);
    t.one(105, servers - 1, false);

    //Re-connect
    t.connect((leader + 1) % servers);

    //The full set of servers should preserve previous agreements, and be able to agree on new commands
    t.one(106, servers, true);
    sleepMs(RAFT_ELECTION_TIMEOUT_MS);
    t.one(107, servers, true);
    t.end();
}


//No agreement if too many followers disconnect
void testFailNoAgree3B(){
    int servers = 5;
    RaftTest t(servers, true);
    t.begin("Test (3B): no agreement if too many followers disconnect");

    t.one(10, servers, false);

    //3 of 5 followers disconnect
    int leader = t.checkOneLeader();
    t.disconnect((leader + 1) % servers);
    t.disconnect((leader + 2) % servers);
    t.disconnect((leader + 3) % servers);

    auto [index, term, ok] = t.rafts[leader]->start(20);
    check(ok, "leader rejected start()");
    check(index == 2, "expected index 2, got " + to_string(index));

    sleepMs(2 * RAFT_ELECTION_TIMEOUT_MS);

    auto [n, cmd] = t.nCommitted(index);
    check(n == 0, to_string(n) + " committed but no majority");

    //Repair
    t.connect((leader + 1) % servers);
    t.connect((leader + 2) % servers);
    t.connect((leader + 3) % servers);

    //The disconnected majority may have chosen a leader from among their own ranks, forgetting index 2
    int leader2 = t.checkOneLeader();
    auto [index2, term2, ok2] = t.rafts[leader2]->start(30);
    check(ok2, "leader2 rejected start()");
    check(index2 >= 2 && index2 <= 3, "unexpected index " + to_string(index2));

    t.one(1000, servers, true);
    t.end();
}


void testConcurrentStarts3B(){
    int servers = 3;
    RaftTest t(servers, true);
    t.begin("Test (3B): concurrent start()s");

    bool success = false;
    for(int attempt = 0; attempt < 5 && !success; attempt ++){
        if(attempt > 0){
            sleepMs(3000);   //give the solution some time to settle
        }

        int leader = t.checkOneLeader();
        auto [i0, term, ok] = t.rafts[leader]->start(1);
        if(!ok){
            continue;   //leader moved on really quickly
        }

        //Fire off a bunch of starts at once
        int iters = 5;
        mutex mu;
        vector<int> indices;
        bool term_changed = false;
        vector<thread> threads;

        for(int i = 0; i < iters; i ++){
            threads.emplace_back([&, i](){
                auto [index, term1, ok1] = t.rafts[leader]->start(100 + i);
                lock_guard<mutex> lock(mu);
                if(term1 != term){
                    term_changed = true;
                }
                if(ok1){
                    indices.push_back(index);
                }
            });
        }
        for(thread &th : threads){
            th.join();
        }

        if(term_changed){
            continue;
        }
        for(int j = 0; j < servers; j ++){
            if(t.rafts[j]->getState().first != term){
                term_changed = true;   //term changed - can't expect low RPC counts
            }
        }
        if(term_changed){
            continue;
        }

        bool failed = false;
        vector<int> cmds;
        for(int index : indices){
            json cmd = t.wait(index, servers, term);
            if(cmd == -1){
                failed = true;   //peers have moved on to later terms, so we can't expect all start()s to work
                break;
            }
            cmds.push_back(cmd);
        }
        if(failed){
            continue;
        }

        for(int i = 0; i < iters; i ++){
            bool found = false;
            for(int c : cmds){
                if(c == 100 + i){
                    found = true;
                }
            }
            check(found, "cmd " + to_string(100 + i) + " missing");
        }
        success = true;
    }

    check(success, "term changed too often");
    t.end();
}


//A partitioned leader that took commands it couldn't commit should get its log fixed when it rejoins
void testRejoin3B(){
    int servers = 3;
    RaftTest t(servers, true);
    t.begin("Test (3B): rejoin of partitioned leader");

    t.one(101, servers, true);

    //Leader network failure
    int leader1 = t.checkOneLeader();
    t.disconnect(leader1);

    //Make old leader try to agree on some entries
    t.rafts[leader1]->start(102);
    t.rafts[leader1]->start(103);
    t.rafts[leader1]->start(104);

    //New leader commits, also for index=2
    t.one(103, 2, true);

    //New leader network failure
    int leader2 = t.checkOneLeader();
    t.disconnect(leader2);

    //Old leader connected again
    t.connect(leader1);
    t.one(104, 2, true);

    //All together now
    t.connect(leader2);
    t.one(105, servers, true);
    t.end();
}


//Leader backs up quickly over incorrect follower logs
void testBackup3B(){
    int servers = 5;
    RaftTest t(servers, true);
    t.begin("Test (3B): leader backs up quickly over incorrect follower logs");

    t.one(randInt(0, 1 << 30), servers, true);

    //Put leader and one follower in a partition
    int leader1 = t.checkOneLeader();
    t.disconnect((leader1 + 2) % servers);
    t.disconnect((leader1 + 3) % servers);
    t.disconnect((leader1 + 4) % servers);

    //Submit lots of commands that won't commit
    for(int i = 0; i < 50; i ++){
        t.rafts[leader1]->start(randInt(0, 1 << 30));
    }
    sleepMs(RAFT_ELECTION_TIMEOUT_MS / 2);

    t.disconnect((leader1 + 0) % servers);
    t.disconnect((leader1 + 1) % servers);

    //Allow other partition to recover
    t.connect((leader1 + 2) % servers);
    t.connect((leader1 + 3) % servers);
    t.connect((leader1 + 4) % servers);

    //Lots of successful commands to new group
    for(int i = 0; i < 50; i ++){
        t.one(randInt(0, 1 << 30), 3, true);
    }

    //Now another partitioned leader and one follower
    int leader2 = t.checkOneLeader();
    int other = (leader1 + 2) % servers;
    if(leader2 == other){
        other = (leader2 + 1) % servers;
    }
    t.disconnect(other);

    //Lots more commands that won't commit
    for(int i = 0; i < 50; i ++){
        t.rafts[leader2]->start(randInt(0, 1 << 30));
    }
    sleepMs(RAFT_ELECTION_TIMEOUT_MS / 2);

    //Bring original leader back to life
    for(int i = 0; i < servers; i ++){
        t.disconnect(i);
    }
    t.connect((leader1 + 0) % servers);
    t.connect((leader1 + 1) % servers);
    t.connect(other);

    //Lots of successful commands to new group
    for(int i = 0; i < 50; i ++){
        t.one(randInt(0, 1 << 30), 3, true);
    }

    //Now everyone
    for(int i = 0; i < servers; i ++){
        t.connect(i);
    }
    t.one(randInt(0, 1 << 30), servers, true);
    t.end();
}


//RPC counts aren't too high
void testCount3B(){
    int servers = 3;
    RaftTest t(servers, true);
    t.begin("Test (3B): RPC counts aren't too high");

    auto rpcs = [&](){
        int n = 0;
        for(int j = 0; j < servers; j ++){
            n += t.rpcCount(j);
        }
        return n;
    };

    int leader = t.checkOneLeader();
    int total1 = rpcs();
    check(total1 <= 30 && total1 >= 1, "too many or few RPCs (" + to_string(total1) + ") to elect initial leader");

    bool success = false;
    int total2 = 0;
    for(int attempt = 0; attempt < 5 && !success; attempt ++){
        if(attempt > 0){
            sleepMs(3000);   //give the solution some time to settle
        }

        leader = t.checkOneLeader();
        total1 = rpcs();

        int iters = 10;
        auto [starti, term, ok] = t.rafts[leader]->start(1);
        if(!ok){
            continue;   //leader moved on really quickly
        }

        vector<int> cmds;
        bool retry = false;
        for(int i = 1; i < iters + 2; i ++){
            int x = randInt(0, 1 << 30);
            cmds.push_back(x);
            auto [index1, term1, ok1] = t.rafts[leader]->start(x);
            if(term1 != term || !ok1){
                retry = true;   //term changed / no longer leader, so term will have changed
                break;
            }
            check(starti + i == index1, "start() failed");
        }
        if(retry){
            continue;
        }

        for(int i = 1; i < iters + 1; i ++){
            json cmd = t.wait(starti + i, servers, term);
            if(cmd == -1){
                retry = true;   //term changed - try again
                break;
            }
            check(cmd == cmds[i - 1], "wrong value " + cmd.dump() + " committed for index " +
                  to_string(starti + i) + "; expected " + to_string(cmds[i - 1]));
        }
        if(retry){
            continue;
        }

        for(int j = 0; j < servers; j ++){
            if(t.rafts[j]->getState().first != term){
                retry = true;   //term changed - can't expect low RPC counts
            }
        }
        if(retry){
            continue;
        }

        total2 = rpcs();
        check(total2 - total1 <= (iters + 1 + 3) * 3, "too many RPCs (" + to_string(total2 - total1) +
              ") for " + to_string(iters) + " entries");
        success = true;
    }
    check(success, "term changed too often");

    sleepMs(RAFT_ELECTION_TIMEOUT_MS);
    int total3 = rpcs();
    check(total3 - total2 <= 3 * 20, "too many RPCs (" + to_string(total3 - total2) + ") for 1 second of idleness");
    t.end();
}


// ---------- 3C: persistence ----------

void testPersist13C(){
    int servers = 3;
    RaftTest t(servers, true);
    t.begin("Test (3C): basic persistence");

    t.one(11, servers, true);

    //Crash and re-start all
    for(int i = 0; i < servers; i ++){
        t.start1(i);
    }
    for(int i = 0; i < servers; i ++){
        t.disconnect(i);
        t.connect(i);
    }

    t.one(12, servers, true);

    int leader1 = t.checkOneLeader();
    t.disconnect(leader1);
    t.start1(leader1);
    t.connect(leader1);

    t.one(13, servers, true);

    int leader2 = t.checkOneLeader();
    t.disconnect(leader2);
    t.one(14, servers - 1, true);
    t.start1(leader2);
    t.connect(leader2);

    t.wait(4, servers, -1);   //wait for leader2 to join before killing i3

    int i3 = (t.checkOneLeader() + 1) % servers;
    t.disconnect(i3);
    t.one(15, servers - 1, true);
    t.start1(i3);
    t.connect(i3);

    t.one(16, servers, true);
    t.end();
}


void testPersist23C(){
    int servers = 5;
    RaftTest t(servers, true);
    t.begin("Test (3C): more persistence");

    int index = 1;
    for(int iters = 0; iters < 5; iters ++){
        t.one(10 + index, servers, true);
        index ++;

        int leader1 = t.checkOneLeader();

        t.disconnect((leader1 + 1) % servers);
        t.disconnect((leader1 + 2) % servers);

        t.one(10 + index, servers - 2, true);
        index ++;

        t.disconnect((leader1 + 0) % servers);
        t.disconnect((leader1 + 3) % servers);
        t.disconnect((leader1 + 4) % servers);

        t.start1((leader1 + 1) % servers);
        t.start1((leader1 + 2) % servers);
        t.connect((leader1 + 1) % servers);
        t.connect((leader1 + 2) % servers);

        sleepMs(RAFT_ELECTION_TIMEOUT_MS);

        t.start1((leader1 + 3) % servers);
        t.connect((leader1 + 3) % servers);

        t.one(10 + index, servers - 2, true);
        index ++;

        t.connect((leader1 + 4) % servers);
        t.connect((leader1 + 0) % servers);
    }

    t.one(1000, servers, true);
    t.end();
}


void testPersist33C(){
    int servers = 3;
    RaftTest t(servers, true);
    t.begin("Test (3C): partitioned leader and one follower crash, leader restarts");

    t.one(101, 3, true);

    int leader = t.checkOneLeader();
    t.disconnect((leader + 2) % servers);

    t.one(102, 2, true);

    t.crash1((leader + 0) % servers);
    t.crash1((leader + 1) % servers);
    t.connect((leader + 2) % servers);
    t.start1((leader + 0) % servers);
    t.connect((leader + 0) % servers);

    t.one(103, 2, true);

    t.start1((leader + 1) % servers);
    t.connect((leader + 1) % servers);

    t.one(104, servers, true);
    t.end();
}


//Test the scenarios described in Figure 8 of the extended Raft paper. Each iteration asks a leader,
//if there is one, to insert a command in the Raft log. If there is a leader, that leader will fail
//quickly with a high probability (perhaps without committing the command), or crash after a while
//with low probability (most likely committing the command). If the number of alive servers isn't
//enough to form a majority, perhaps start a new server. The leader in a new term may try to finish
//replicating log entries that haven't been committed yet.
void testFigure83C(){
    int servers = 5;
    RaftTest t(servers, true);
    t.begin("Test (3C): Figure 8");

    t.one(randInt(0, 1 << 30), 1, true);

    int nup = servers;
    for(int iters = 0; iters < 1000; iters ++){
        int leader = -1;
        for(int i = 0; i < servers; i ++){
            auto rf = t.getRaft(i);
            if(rf){
                auto [index, term, ok] = rf->start(randInt(0, 1 << 30));
                if(ok){
                    leader = i;
                }
            }
        }

        if(randInt(0, 1000) < 100){
            sleepMs(randInt(0, RAFT_ELECTION_TIMEOUT_MS / 2));
        }
        else {
            sleepMs(randInt(0, 13));
        }

        if(leader != -1){
            t.crash1(leader);
            nup --;
        }

        if(nup < 3){
            int s = randInt(0, servers);
            if(!t.getRaft(s)){
                t.start1(s);
                t.connect(s);
                nup ++;
            }
        }
    }

    for(int i = 0; i < servers; i ++){
        if(!t.getRaft(i)){
            t.start1(i);
            t.connect(i);
        }
    }

    t.one(randInt(0, 1 << 30), servers, true);
    t.end();
}


void testUnreliableAgree3C(){
    int servers = 5;
    RaftTest t(servers, false);
    t.begin("Test (3C): unreliable agreement");

    vector<thread> threads;
    for(int iters = 1; iters < 50; iters ++){
        for(int j = 0; j < 4; j ++){
            threads.emplace_back([&t, iters, j](){
                t.one(100 * iters + j, 1, true);
            });
        }
        t.one(iters, 1, true);
    }
    for(thread &th : threads){
        th.join();
    }

    t.setUnreliable(false);
    t.one(100, servers, true);
    t.end();
}


void testFigure8Unreliable3C(){
    int servers = 5;
    RaftTest t(servers, false);
    t.begin("Test (3C): Figure 8 (unreliable)");

    t.one(randInt(0, 10000), 1, true);

    int nup = servers;
    for(int iters = 0; iters < 1000; iters ++){
        if(iters == 200){
            t.setLongReordering(true);
        }

        int leader = -1;
        for(int i = 0; i < servers; i ++){
            auto [index, term, ok] = t.rafts[i]->start(randInt(0, 10000));
            if(ok && t.connected[i]){
                leader = i;
            }
        }

        if(randInt(0, 1000) < 100){
            sleepMs(randInt(0, RAFT_ELECTION_TIMEOUT_MS / 2));
        }
        else {
            sleepMs(randInt(0, 13));
        }

        if(leader != -1 && randInt(0, 1000) < RAFT_ELECTION_TIMEOUT_MS / 2){
            t.disconnect(leader);
            nup --;
        }

        if(nup < 3){
            int s = randInt(0, servers);
            if(!t.connected[s]){
                t.connect(s);
                nup ++;
            }
        }
    }

    for(int i = 0; i < servers; i ++){
        if(!t.connected[i]){
            t.connect(i);
        }
    }

    t.one(randInt(0, 10000), servers, true);
    t.end();
}


//Clients keep submitting while servers get randomly disconnected, crashed and restarted. At the end,
//everything a client saw committed has to actually be in the final log.
void internalChurn(bool unreliable){
    int servers = 5;
    RaftTest t(servers, !unreliable);
    if(unreliable){
        t.begin("Test (3C): unreliable churn");
    }
    else {
        t.begin("Test (3C): churn");
    }

    atomic<bool> stop{false};
    int ncli = 3;
    vector<vector<int>> values(ncli);
    vector<thread> clients;

    for(int me = 0; me < ncli; me ++){
        clients.emplace_back([&, me](){
            while(!stop){
                int x = randInt(0, 1 << 30);
                int index = -1;
                bool ok = false;

                for(int i = 0; i < servers; i ++){
                    auto rf = t.getRaft(i);
                    if(rf){
                        auto [index1, term1, ok1] = rf->start(x);
                        if(ok1){
                            ok = true;
                            index = index1;
                        }
                    }
                }

                if(ok){
                    //Maybe leader will commit our value, maybe not. But don't wait forever.
                    for(int to : { 10, 20, 50, 100, 200 }){
                        auto [nd, cmd] = t.nCommitted(index);
                        if(nd > 0){
                            if(cmd == x){
                                values[me].push_back(x);
                            }
                            break;
                        }
                        sleepMs(to);
                    }
                }
                else {
                    sleepMs(79 + me * 17);
                }
            }
        });
    }

    for(int iters = 0; iters < 20; iters ++){
        if(randInt(0, 1000) < 200){
            t.disconnect(randInt(0, servers));
        }

        if(randInt(0, 1000) < 500){
            int i = randInt(0, servers);
            if(!t.getRaft(i)){
                t.start1(i);
            }
            t.connect(i);
        }

        if(randInt(0, 1000) < 200){
            int i = randInt(0, servers);
            if(t.getRaft(i)){
                t.crash1(i);
            }
        }

        //Make crash/restart infrequent enough that the peers can often keep up, but not so infrequent
        //that everything has settled down from one change to the next
        sleepMs(RAFT_ELECTION_TIMEOUT_MS * 7 / 10);
    }

    sleepMs(RAFT_ELECTION_TIMEOUT_MS);
    t.setUnreliable(false);
    for(int i = 0; i < servers; i ++){
        if(!t.getRaft(i)){
            t.start1(i);
        }
        t.connect(i);
    }

    stop = true;
    for(thread &c : clients){
        c.join();
    }

    sleepMs(RAFT_ELECTION_TIMEOUT_MS);

    int last_index = t.one(randInt(0, 1 << 30), servers, true);

    set<int> really;
    for(int index = 1; index <= last_index; index ++){
        json v = t.wait(index, servers, -1);
        if(v.is_number_integer()){
            really.insert(v.get<int>());
        }
    }

    for(auto &vs : values){
        for(int v : vs){
            check(really.count(v), "didn't find a value");
        }
    }
    t.end();
}


// ---------- 3D: snapshots ----------

//Shared body for the snapshot tests - keeps a victim server disconnected or crashed while the others
//commit enough to snapshot past it, then brings it back so it has to catch up from a snapshot
void snapCommon(string name, bool disconnect, bool reliable, bool crash){
    int iters = 30;
    int servers = 3;
    RaftTest t(servers, reliable, true);
    t.begin(name);

    t.one(randInt(0, 1 << 30), servers, true);
    int leader1 = t.checkOneLeader();

    for(int i = 0; i < iters; i ++){
        int victim = (leader1 + 1) % servers;
        int sender = leader1;
        if(i % 3 == 1){
            sender = (leader1 + 1) % servers;
            victim = leader1;
        }

        if(disconnect){
            t.disconnect(victim);
            t.one(randInt(0, 1 << 30), servers - 1, true);
        }
        if(crash){
            t.crash1(victim);
            t.one(randInt(0, 1 << 30), servers - 1, true);
        }

        //Perhaps send enough to get a snapshot
        int nn = SNAPSHOT_INTERVAL / 2 + randInt(0, SNAPSHOT_INTERVAL);
        for(int j = 0; j < nn; j ++){
            auto rf = t.getRaft(sender);
            if(rf){
                rf->start(randInt(0, 1 << 30));
            }
        }

        //Let applier threads catch up with the start()s
        if(!disconnect && !crash){
            t.one(randInt(0, 1 << 30), servers, true);   //make sure all followers have caught up
        }
        else {
            t.one(randInt(0, 1 << 30), servers - 1, true);
        }

        check(t.logSize() < MAX_LOG_SIZE, "log size too large (" + to_string(t.logSize()) + ")");

        if(disconnect){
            //Reconnect a follower, who maybe behind and needs to receive a snapshot to catch up
            t.connect(victim);
            t.one(randInt(0, 1 << 30), servers, true);
            leader1 = t.checkOneLeader();
        }
        if(crash){
            t.start1(victim);
            t.connect(victim);
            t.one(randInt(0, 1 << 30), servers, true);
            leader1 = t.checkOneLeader();
        }
    }
    t.end();
}


//Crash and restart all servers repeatedly, they should come back from their snapshots
void testSnapshotAllCrash3D(){
    int servers = 3;
    int iters = 5;
    RaftTest t(servers, false, true);
    t.begin("Test (3D): crash and restart all servers");

    t.one(randInt(0, 1 << 30), servers, true);

    for(int i = 0; i < iters; i ++){
        //Perhaps enough to get a snapshot
        int nn = SNAPSHOT_INTERVAL / 2 + randInt(0, SNAPSHOT_INTERVAL);
        for(int j = 0; j < nn; j ++){
            t.one(randInt(0, 1 << 30), servers, true);
        }

        int index1 = t.one(randInt(0, 1 << 30), servers, true);

        //Crash all
        for(int j = 0; j < servers; j ++){
            t.crash1(j);
        }

        //Revive all
        for(int j = 0; j < servers; j ++){
            t.start1(j);
            t.connect(j);
        }

        int index2 = t.one(randInt(0, 1 << 30), servers, true);
        check(index2 >= index1 + 1, "index decreased from " + to_string(index1) + " to " + to_string(index2));
    }
    t.end();
}


//Do servers correctly initialize their in-memory copy of the snapshot, making sure that future
//writes to persistent state don't lose state?
void testSnapshotInit3D(){
    int servers = 3;
    RaftTest t(servers, false, true);
    t.begin("Test (3D): snapshot initialization after crash");

    t.one(randInt(0, 1 << 30), servers, true);

    //Enough ops to make a snapshot
    int nn = SNAPSHOT_INTERVAL + 1;
    for(int i = 0; i < nn; i ++){
        t.one(randInt(0, 1 << 30), servers, true);
    }

    //Crash all
    for(int i = 0; i < servers; i ++){
        t.crash1(i);
    }
    for(int i = 0; i < servers; i ++){
        t.start1(i);
        t.connect(i);
    }

    //A single op, to get something to be written back to persistent storage
    t.one(randInt(0, 1 << 30), servers, true);

    //Crash all again
    for(int i = 0; i < servers; i ++){
        t.crash1(i);
    }
    for(int i = 0; i < servers; i ++){
        t.start1(i);
        t.connect(i);
    }

    //Do another op to trigger potential bug
    t.one(randInt(0, 1 << 30), servers, true);
    t.end();
}


int main(int argc, char** argv){
    return runTests({
        { "TestInitialElection3A", testInitialElection3A },
        { "TestReElection3A", testReElection3A },
        { "TestManyElections3A", testManyElections3A },
        { "TestBasicAgree3B", testBasicAgree3B },
        { "TestRPCBytes3B", testRPCBytes3B },
        { "TestFollowerFailure3B", testFollowerFailure3B },
        { "TestLeaderFailure3B", testLeaderFailure3B },
        { "TestFailAgree3B", testFailAgree3B },
        { "TestFailNoAgree3B", testFailNoAgree3B },
        { "TestConcurrentStarts3B", testConcurrentStarts3B },
        { "TestRejoin3B", testRejoin3B },
        { "TestBackup3B", testBackup3B },
        { "TestCount3B", testCount3B },
        { "TestPersist13C", testPersist13C },
        { "TestPersist23C", testPersist23C },
        { "TestPersist33C", testPersist33C },
        { "TestFigure83C", testFigure83C },
        { "TestUnreliableAgree3C", testUnreliableAgree3C },
        { "TestFigure8Unreliable3C", testFigure8Unreliable3C },
        { "TestReliableChurn3C", [](){ internalChurn(false); } },
        { "TestUnreliableChurn3C", [](){ internalChurn(true); } },
        { "TestSnapshotBasic3D", [](){ snapCommon("Test (3D): snapshots basic", false, true, false); } },
        { "TestSnapshotInstall3D", [](){ snapCommon("Test (3D): install snapshots (disconnect)", true, true, false); } },
        { "TestSnapshotInstallUnreliable3D", [](){ snapCommon("Test (3D): install snapshots (disconnect+unreliable)", true, false, false); } },
        { "TestSnapshotInstallCrash3D", [](){ snapCommon("Test (3D): install snapshots (crash)", false, true, true); } },
        { "TestSnapshotInstallUnCrash3D", [](){ snapCommon("Test (3D): install snapshots (unreliable+crash)", false, false, true); } },
        { "TestSnapshotAllCrash3D", testSnapshotAllCrash3D },
        { "TestSnapshotInit3D", testSnapshotInit3D },
    }, argc, argv);
}
