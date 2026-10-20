#include <vector>
#include <thread>
#include <atomic>
#include <set>
#include "../../common/testing.hpp"
#include "../../common/linearizability.hpp"
#include "../rsm.hpp"
#include "../server.hpp"
#include "../client.hpp"
#include "group.hpp"

using namespace std;

//Lab 4 tests - 4A tests the rsm layer on its own with a counter state machine, 4B/4C test the kv
//server on top of it. Every kv test records each client op and runs the linearizability checker at the end.


// ---------- 4A: rsm with a counter ----------

//Simplest possible state machine - one counter, "inc" bumps it and returns the new value
class CounterSM : public StateMachine {
    public:
        int counter = 0;

        json doOp(json req) override {
            if(req["op"] == "inc"){
                counter ++;
            }
            return counter;
        }

        string snapshot() override {
            return to_string(counter);
        }

        void restore(string data) override {
            counter = stoi(data);
        }
};


struct RsmServer {
    CounterSM sm;
    unique_ptr<RSM> rsm;    //declared after sm so it's destroyed first
};


ServiceFactory rsmFactory(){
    return [](vector<shared_ptr<ClientEnd>> peers, int me, shared_ptr<Persister> persister, int max_raft_state){
        auto srv = make_shared<RsmServer>();
        srv->rsm = make_unique<RSM>(peers, me, persister, max_raft_state, &srv->sm);

        ServiceHandle handle;
        handle.service = srv;
        handle.raft = srv->rsm->raft();
        handle.server = handle.raft->makeServer();
        handle.kill = [srv](){ srv->rsm->kill(); };
        return handle;
    };
}


//Grab server i's rsm - the returned shared_ptr keeps the server alive even if it gets shut down meanwhile
shared_ptr<RsmServer> rsmOf(Group &g, int i){
    return static_pointer_cast<RsmServer>(g.service(i).service);
}


//Keep trying servers in `allowed` until one accepts + commits req, returns the result
json submitAny(Group &g, json req, vector<int> allowed = {}){
    if(allowed.empty()){
        for(int i = 0; i < g.size(); i ++){
            allowed.push_back(i);
        }
    }

    auto start = chrono::steady_clock::now();
    while(chrono::steady_clock::now() - start < chrono::seconds(10)){
        for(int i : allowed){
            auto srv = rsmOf(g, i);
            if(!srv){
                continue;
            }
            auto [err, result] = srv->rsm->submit(req);
            if(err == OK){
                return result;
            }
        }
        sleepMs(50);
    }
    check(false, "submit " + req.dump() + " never went through");
    return json();
}


json inc(){
    json req;
    req["op"] = "inc";
    return req;
}


void testBasic4A(){
    auto net = make_shared<Network>();
    Group g(net, 0, 3, -1, rsmFactory());

    for(int i = 1; i <= 10; i ++){
        check(submitAny(g, inc()) == i, "counter should be " + to_string(i));
    }
}


//Lots of submits at once - every one should get a different counter value
void testConcurrent4A(){
    auto net = make_shared<Network>();
    Group g(net, 0, 3, -1, rsmFactory());
    g.waitForLeader();

    int n = 50;
    mutex mu;
    set<int> results;
    vector<thread> threads;
    for(int i = 0; i < n; i ++){
        threads.emplace_back([&](){
            int r = submitAny(g, inc());
            lock_guard<mutex> lock(mu);
            check(!results.count(r), "two submits got the same result " + to_string(r));
            results.insert(r);
        });
    }
    for(thread &t : threads){
        t.join();
    }

    check(results.size() == n && *results.begin() == 1 && *results.rbegin() == n, "results should be 1.." + to_string(n));
}


void testLeaderFailure4A(){
    auto net = make_shared<Network>();
    Group g(net, 0, 3, -1, rsmFactory());

    check(submitAny(g, inc()) == 1, "first inc");

    int l = g.waitForLeader();
    auto [rest, alone] = g.isolate(l);
    g.partition(rest, alone);

    check(submitAny(g, inc(), rest) == 2, "inc on the new leader");

    g.connectAll();
    check(submitAny(g, inc()) == 3, "inc after the old leader rejoins");
}


//A submit to a leader stuck in a minority can't commit - once it rejoins and finds out there's a new
//leader, the submit has to give up with ErrWrongLeader instead of hanging or returning someone else's result
void testLeaderPartition4A(){
    auto net = make_shared<Network>();
    Group g(net, 0, 3, -1, rsmFactory());

    check(submitAny(g, inc()) == 1, "first inc");

    int l = g.waitForLeader();
    auto [rest, alone] = g.isolate(l);
    g.partition(rest, alone);

    atomic<bool> done{false};
    string minority_err;
    auto old_leader = rsmOf(g, l);
    thread minority([&](){
        auto [err, result] = old_leader->rsm->submit(inc());
        minority_err = err;
        done = true;
    });

    check(submitAny(g, inc(), rest) == 2, "inc on the majority side");
    sleepMs(300);
    check(!done, "submit in the minority finished while partitioned");

    g.connectAll();
    for(int i = 0; i < 50 && !done; i ++){
        sleepMs(100);
    }
    check(done, "submit to the old leader never returned");
    check(minority_err == ErrWrongLeader, "old leader's submit should be ErrWrongLeader");
    minority.join();

    check(submitAny(g, inc()) == 3, "inc after healing");
}


//Restarted servers rebuild their state by replaying the log
void testRestartReplay4A(){
    auto net = make_shared<Network>();
    Group g(net, 0, 3, -1, rsmFactory());

    for(int i = 1; i <= 10; i ++){
        submitAny(g, inc());
    }

    g.shutdown();
    g.startServers();

    check(submitAny(g, inc()) == 11, "state not restored after restart");
}


//Shutting down a server with a submit in flight shouldn't leave the submit hanging
void testShutdown4A(){
    auto net = make_shared<Network>();
    Group g(net, 0, 3, -1, rsmFactory());
    submitAny(g, inc());

    int l = g.waitForLeader();
    auto [rest, alone] = g.isolate(l);
    g.partition(rest, alone);

    atomic<bool> done{false};
    auto old_leader = rsmOf(g, l);
    thread stuck([&](){
        old_leader->rsm->submit(inc());
        done = true;
    });

    sleepMs(200);
    g.shutdownServer(l);
    for(int i = 0; i < 30 && !done; i ++){
        sleepMs(100);
    }
    check(done, "submit didn't return after shutdown");
    stuck.join();
}


//With a max raft state set, the rsm should snapshot so the log stays small, and come back from it
void testRsmSnapshot4C(){
    int max_raft_state = 1000;
    auto net = make_shared<Network>();
    Group g(net, 0, 3, max_raft_state, rsmFactory());

    for(int i = 1; i <= 100; i ++){
        submitAny(g, inc());
    }
    check(g.logSize() <= 2 * max_raft_state, "log too big (" + to_string(g.logSize()) + "), not snapshotting");

    g.shutdown();
    g.startServers();
    check(submitAny(g, inc()) == 101, "state not restored from snapshot");
}


// ---------- 4B/4C: kv server ----------

ServiceFactory kvFactory(){
    return [](vector<shared_ptr<ClientEnd>> peers, int me, shared_ptr<Persister> persister, int max_raft_state){
        auto kv = make_shared<KVServer>(peers, me, persister, max_raft_state);

        ServiceHandle handle;
        handle.service = kv;
        handle.raft = kv->raft();
        handle.server = kv->makeServer();
        handle.kill = [kv](){ kv->kill(); };
        return handle;
    };
}


//Everything a kv test needs - one group, client ends, and an op history
struct KVTest {
    shared_ptr<Network> net = make_shared<Network>();
    Group group;
    ClientNet clients;
    History history;

    KVTest(int n, bool reliable, int max_raft_state)
        : group(net, 0, n, max_raft_state, kvFactory()), clients(net) {
        net->setReliable(reliable);
    }

    //A clerk with an end to every server. Returns the client id too so tests can partition it.
    pair<unique_ptr<Clerk>, int> makeClerk(){
        int id = clients.newClient();
        vector<shared_ptr<ClientEnd>> ends;
        for(string name : group.serverNames()){
            ends.push_back(clients.makeEnd(id, name));
        }
        return { make_unique<Clerk>(ends), id };
    }

    vector<string> names(vector<int> servers){
        vector<string> out;
        for(int i : servers){
            out.push_back(group.serverName(i));
        }
        return out;
    }

    void checkLinearizable(){
        vector<KVOp> ops = history.all();
        string bad = ::checkLinearizable(ops);
        check(bad.empty(), "history for key " + bad + " is not linearizable (" + to_string(ops.size()) + " ops)");
    }
};


//One client's loop - read a key, try to put a new value at that version, repeat
void clientLoop(KVTest &t, int me, atomic<bool> &stop, bool random_keys, int &puts){
    auto [ck, id] = t.makeClerk();
    RecordingClerk rck(ck.get(), &t.history);

    int n = 0;
    while(!stop){
        string key = random_keys ? "k" + to_string(randInt(0, 10)) : "k";
        GetResult r = rck.get(key);
        uint64_t version = r.err == ErrNoKey ? 0 : r.version;

        string err = rck.put(key, "c" + to_string(me) + "-" + to_string(n ++), version);
        if(err == OK){
            puts ++;
        }
    }
}


//The big one, same shape as GenericTest in the MIT tests. Each of 3 rounds: clients hammer the service
//for a second while (optionally) the network gets re-partitioned every ~second; then (optionally)
//every server crashes and restarts. At the end the whole history has to be linearizable.
void genericTest(int n_clients, bool unreliable, bool crash, bool partitions, int max_raft_state, bool random_keys){
    int servers = 5;
    KVTest t(servers, !unreliable, max_raft_state);

    for(int iter = 0; iter < 3; iter ++){
        atomic<bool> stop{false};
        vector<int> puts(n_clients, 0);
        vector<thread> clients;
        for(int c = 0; c < n_clients; c ++){
            clients.emplace_back([&, c](){ clientLoop(t, c, stop, random_keys, puts[c]); });
        }

        atomic<bool> stop_partitioner{false};
        thread partitioner;
        if(partitions){
            partitioner = thread([&](){
                while(!stop_partitioner){
                    auto [a, b] = t.group.randomPartition();
                    t.group.partition(a, b);
                    sleepMs(ELECTION_TIMEOUT_MS + randInt(0, 200));
                }
            });
        }

        sleepMs(1000);
        stop = true;

        //Heal before waiting on clients - one could be stuck talking to a leader in a minority
        if(partitions){
            stop_partitioner = true;
            partitioner.join();
            t.group.connectAll();
            sleepMs(ELECTION_TIMEOUT_MS);
        }
        for(thread &c : clients){
            c.join();
        }

        int total = 0;
        for(int p : puts){
            total += p;
        }
        check(total > 0, "no put went through in round " + to_string(iter));

        if(crash){
            t.group.shutdown();
            t.group.startServers();
        }

        //Make sure everyone has caught up, then read every key (recorded, so the checker sees the end state)
        auto [ck, id] = t.makeClerk();
        RecordingClerk rck(ck.get(), &t.history);
        for(int k = 0; k < (random_keys ? 10 : 1); k ++){
            rck.get(random_keys ? "k" + to_string(k) : "k");
        }

        if(max_raft_state > 0){
            check(t.group.logSize() <= 8 * max_raft_state, "logs were not trimmed (" + to_string(t.group.logSize()) +
                  " > 8*" + to_string(max_raft_state) + ")");
        }
        if(max_raft_state < 0){
            check(t.group.snapshotSize() == 0, "snapshot taken even though max raft state is -1");
        }
    }

    t.checkLinearizable();
}


//Puts should commit about as fast as raft can do a round trip, not one per heartbeat
void testSpeed(int max_raft_state){
    KVTest t(3, true, max_raft_state);
    auto [ck, id] = t.makeClerk();

    check(ck->put("x", "0", 0) == OK, "first put");

    int n = 1000;
    auto start = chrono::steady_clock::now();
    for(int i = 1; i <= n; i ++){
        check(ck->put("x", to_string(i), i) == OK, "put " + to_string(i));
    }
    double per_op_ms = chrono::duration<double, milli>(chrono::steady_clock::now() - start).count() / n;

    //Must beat 3 ops per heartbeat interval
    check(per_op_ms < 100.0 / 3, "operations completed too slowly " + to_string(per_op_ms) + "ms/op");
    check(ck->get("x").value == to_string(n), "final value");
}


//Put + get on the majority side work, ops on the minority side block until the partition heals
void testOnePartition4B(){
    int servers = 5;
    KVTest t(servers, true, -1);
    auto [ck, ck_id] = t.makeClerk();
    RecordingClerk rck(ck.get(), &t.history);

    check(rck.put("1", "13", 0) == OK, "first put");

    int l = t.group.waitForLeader();
    vector<int> p1 = { l, (l + 1) % servers, (l + 2) % servers };
    vector<int> p2 = { (l + 3) % servers, (l + 4) % servers };
    t.group.partition(p1, p2);

    auto [ckp1, id1] = t.makeClerk();
    auto [ckp2a, id2a] = t.makeClerk();
    auto [ckp2b, id2b] = t.makeClerk();
    t.clients.connectClientTo(id1, t.names(p1));
    t.clients.connectClientTo(id2a, t.names(p2));
    t.clients.connectClientTo(id2b, t.names(p2));
    RecordingClerk rp1(ckp1.get(), &t.history);
    RecordingClerk rp2a(ckp2a.get(), &t.history);
    RecordingClerk rp2b(ckp2b.get(), &t.history);

    check(rp1.put("1", "14", 1) == OK, "put on majority side");
    check(rp1.get("1").value == "14", "get on majority side");

    atomic<bool> done0{false}, done1{false};
    thread minority_put([&](){ rp2a.put("1", "15", 2); done0 = true; });
    thread minority_get([&](){ rp2b.get("1"); done1 = true; });

    sleepMs(1000);
    check(!done0, "put in minority completed");
    check(!done1, "get in minority completed");

    check(rp1.get("1").value == "14", "majority side still sees 14");
    check(rp1.put("1", "16", 2) == OK, "put 16 on majority side");
    check(rp1.get("1").value == "16", "majority side sees 16");

    //Heal - the stuck ops should finish now
    t.group.connectAll();
    t.clients.connectClientAll(id2a);
    t.clients.connectClientAll(id2b);
    sleepMs(ELECTION_TIMEOUT_MS);

    for(int i = 0; i < 300 && !(done0 && done1); i ++){
        sleepMs(100);
    }
    check(done0, "put did not complete after partition healed");
    check(done1, "get did not complete after partition healed");
    minority_put.join();
    minority_get.join();

    rck.get("1");
    t.checkLinearizable();
}


//A server that missed a bunch of ops while partitioned has to catch up with an InstallSnapshot
void testSnapshotRPC4C(){
    int max_raft_state = 1000;
    KVTest t(3, true, max_raft_state);
    auto [ck, ck_id] = t.makeClerk();

    check(ck->put("a", "A", 0) == OK, "put a");
    check(ck->get("a").value == "A", "get a");

    //A bunch of puts into the majority partition
    t.group.partition({ 0, 1 }, { 2 });
    {
        auto [ck1, id1] = t.makeClerk();
        t.clients.connectClientTo(id1, t.names({ 0, 1 }));
        for(int i = 0; i < 50; i ++){
            check(ck1->put(to_string(i), to_string(i), 0) == OK, "put " + to_string(i));
        }
        sleepMs(ELECTION_TIMEOUT_MS);
        check(ck1->put("b", "B", 0) == OK, "put b");
    }

    //Check that the majority partition has thrown away most of its log entries
    check(t.group.logSize() <= 8 * max_raft_state, "logs were not trimmed (" + to_string(t.group.logSize()) + ")");

    //Now make a group that requires participation of the lagging server, so it has to catch up via snapshot
    t.group.partition({ 0, 2 }, { 1 });
    {
        auto [ck1, id1] = t.makeClerk();
        t.clients.connectClientTo(id1, t.names({ 0, 2 }));
        check(ck1->put("c", "C", 0) == OK, "put c");
        check(ck1->put("d", "D", 0) == OK, "put d");
        check(ck1->get("a").value == "A", "get a");
        check(ck1->get("b").value == "B", "get b");
        check(ck1->get("1").value == "1", "get 1");
        check(ck1->get("49").value == "49", "get 49");
    }

    //Now everybody
    t.group.connectAll();
    check(ck->put("e", "E", 0) == OK, "put e");
    check(ck->get("c").value == "C", "get c");
    check(ck->get("e").value == "E", "get e");
    check(ck->get("1").value == "1", "get 1");
}


//Snapshots + logs should stay small when one key keeps getting overwritten
void testSnapshotSize4C(){
    int max_raft_state = 1000;
    int max_snapshot_state = 500;
    KVTest t(3, true, max_raft_state);
    auto [ck, ck_id] = t.makeClerk();

    for(int i = 0; i < 200; i ++){
        check(ck->put("x", to_string(i % 2), i) == OK, "put");
    }

    check(t.group.logSize() <= 8 * max_raft_state, "logs were not trimmed (" + to_string(t.group.logSize()) + ")");
    check(t.group.snapshotSize() <= max_snapshot_state, "snapshot too large (" + to_string(t.group.snapshotSize()) + ")");
}


int main(int argc, char** argv){
    return runTests({
        { "TestBasic4A", testBasic4A },
        { "TestConcurrent4A", testConcurrent4A },
        { "TestLeaderFailure4A", testLeaderFailure4A },
        { "TestLeaderPartition4A", testLeaderPartition4A },
        { "TestRestartReplay4A", testRestartReplay4A },
        { "TestShutdown4A", testShutdown4A },
        { "TestRsmSnapshot4C", testRsmSnapshot4C },

        { "TestBasic4B", [](){ genericTest(1, false, false, false, -1, true); } },
        { "TestSpeed4B", [](){ testSpeed(-1); } },
        { "TestConcurrent4B", [](){ genericTest(5, false, false, false, -1, true); } },
        { "TestUnreliable4B", [](){ genericTest(5, true, false, false, -1, true); } },
        { "TestUnreliableOneKey4B", [](){ genericTest(5, true, false, false, -1, false); } },
        { "TestOnePartition4B", testOnePartition4B },
        { "TestManyPartitionsOneClient4B", [](){ genericTest(1, false, false, true, -1, true); } },
        { "TestManyPartitionsManyClients4B", [](){ genericTest(5, false, false, true, -1, true); } },
        { "TestPersistOneClient4B", [](){ genericTest(1, false, true, false, -1, true); } },
        { "TestPersistConcurrent4B", [](){ genericTest(5, false, true, false, -1, true); } },
        { "TestPersistConcurrentUnreliable4B", [](){ genericTest(5, true, true, false, -1, true); } },
        { "TestPersistPartition4B", [](){ genericTest(5, false, true, true, -1, true); } },
        { "TestPersistPartitionUnreliable4B", [](){ genericTest(5, true, true, true, -1, true); } },
        { "TestPersistPartitionUnreliableLinearizable4B", [](){ genericTest(15, true, true, true, -1, false); } },

        { "TestSnapshotRPC4C", testSnapshotRPC4C },
        { "TestSnapshotSize4C", testSnapshotSize4C },
        { "TestSpeed4C", [](){ testSpeed(1000); } },
        { "TestSnapshotRecover4C", [](){ genericTest(1, false, true, false, 1000, true); } },
        { "TestSnapshotRecoverManyClients4C", [](){ genericTest(20, false, true, false, 1000, true); } },
        { "TestSnapshotUnreliable4C", [](){ genericTest(5, true, false, false, 1000, true); } },
        { "TestSnapshotUnreliableRecover4C", [](){ genericTest(5, true, true, false, 1000, true); } },
        { "TestSnapshotUnreliableRecoverConcurrentPartition4C", [](){ genericTest(5, true, true, true, 1000, true); } },
        { "TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable4C", [](){ genericTest(15, true, true, true, 1000, false); } },
    }, argc, argv);
}
