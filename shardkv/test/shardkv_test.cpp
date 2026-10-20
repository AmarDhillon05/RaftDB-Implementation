#include <vector>
#include <thread>
#include <atomic>
#include <map>
#include "../../common/testing.hpp"
#include "../../common/linearizability.hpp"
#include "../../kvraft/server.hpp"
#include "../../kvraft/client.hpp"
#include "../../kvraft/test/group.hpp"
#include "../shardcfg.hpp"
#include "../shardctrler.hpp"
#include "../client.hpp"
#include "../shardgrp/server.hpp"

using namespace std;

//Lab 5 tests - 5A moving shards, 5B a controller crashing mid-change, 5C several controllers at once.
//The controller keeps its configs in a lab 4 kv group (gid 0); shard groups are gid 1, 2, 3, ...


const int CTRL_GID = 0;


ServiceFactory ctrlKVFactory(){
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


ServiceFactory shardGrpFactory(int gid){
    return [gid](vector<shared_ptr<ClientEnd>> peers, int me, shared_ptr<Persister> persister, int max_raft_state){
        auto grp = make_shared<ShardGrp>(peers, me, persister, max_raft_state, gid);
        ServiceHandle handle;
        handle.service = grp;
        handle.raft = grp->raft();
        handle.server = grp->makeServer();
        handle.kill = [grp](){ grp->kill(); };
        return handle;
    };
}


//A controller plus the kv clerk it stores configs through
struct Controller {
    int client_id;
    unique_ptr<Clerk> kv;
    unique_ptr<ShardCtrler> sck;
};


//A client - it needs its own controller handle to look up configs
struct ShardClient {
    unique_ptr<Controller> ctrl;
    unique_ptr<ShardClerk> ck;
};


struct ShardTest {
    shared_ptr<Network> net = make_shared<Network>();
    ClientNet clients;
    History history;
    int max_raft_state;
    unique_ptr<Group> ctrl_group;
    map<int, unique_ptr<Group>> groups;
    unique_ptr<Controller> ctrl;    //the test's main controller


    ShardTest(bool reliable, int max_raft_state = -1) : clients(net) {
        this->max_raft_state = max_raft_state;
        net->setReliable(reliable);

        ctrl_group = make_unique<Group>(net, CTRL_GID, 3, -1, ctrlKVFactory());
        addGroup(GID1);

        ctrl = makeController();
        ctrl->sck->initConfig(ShardConfig::initial(groups[GID1]->serverNames()));
    }


    ~ShardTest(){
        //Shard groups first, they don't depend on anything else
        groups.clear();
        ctrl_group.reset();
    }


    void addGroup(int gid){
        if(!groups.count(gid)){
            groups[gid] = make_unique<Group>(net, gid, 3, max_raft_state, shardGrpFactory(gid));
        }
    }


    unique_ptr<Controller> makeController(){
        auto c = make_unique<Controller>();
        c->client_id = clients.newClient();
        vector<shared_ptr<ClientEnd>> ends;
        for(string name : ctrl_group->serverNames()){
            ends.push_back(clients.makeEnd(c->client_id, name));
        }
        c->kv = make_unique<Clerk>(ends);

        int id = c->client_id;
        c->sck = make_unique<ShardCtrler>(c->kv.get(), [this, id](string name){ return clients.makeEnd(id, name); });
        return c;
    }


    unique_ptr<ShardClient> makeClerk(){
        auto c = make_unique<ShardClient>();
        c->ctrl = makeController();
        int id = c->ctrl->client_id;
        c->ck = make_unique<ShardClerk>(c->ctrl->sck.get(), [this, id](string name){ return clients.makeEnd(id, name); });
        return c;
    }


    ShardConfig query(){
        return ctrl->sck->query();
    }


    //Start the groups if needed and move to a config that includes them
    void join(vector<int> gids, Controller* c = nullptr){
        c = c ? c : ctrl.get();
        map<int, vector<string>> new_groups;
        for(int gid : gids){
            addGroup(gid);
            new_groups[gid] = groups[gid]->serverNames();
        }
        c->sck->changeConfigTo(c->sck->query().join(new_groups));
    }


    void leave(vector<int> gids, Controller* c = nullptr){
        c = c ? c : ctrl.get();
        c->sck->changeConfigTo(c->sck->query().leave(gids));
    }


    //Only the controller kv servers - used to cut a controller off from the shard groups
    vector<string> ctrlServerNames(){
        return ctrl_group->serverNames();
    }


    void checkLinearizable(){
        vector<KVOp> ops = history.all();
        string bad = ::checkLinearizable(ops);
        check(bad.empty(), "history for key " + bad + " is not linearizable (" + to_string(ops.size()) + " ops)");
    }
};


//Helper - puts n keys (k0..k(n-1)) and returns what each was set to + its version
map<string, pair<string, uint64_t>> putKeys(IKVClerk* ck, int n, string prefix = "v"){
    map<string, pair<string, uint64_t>> kvs;
    for(int i = 0; i < n; i ++){
        string key = "k" + to_string(i);
        string value = prefix + to_string(i);
        check(ck->put(key, value, 0) == OK, "put " + key);
        kvs[key] = { value, 1 };
    }
    return kvs;
}


void checkKeys(IKVClerk* ck, map<string, pair<string, uint64_t>> &kvs){
    for(auto &[key, vv] : kvs){
        GetResult r = ck->get(key);
        check(r.err == OK, "get " + key + " err " + r.err);
        check(r.value == vv.first && r.version == vv.second, "get " + key + " = " + r.value + "@" +
              to_string(r.version) + ", expected " + vv.first + "@" + to_string(vv.second));
    }
}


//Overwrites every key once (at its current version) so later checks see the group really serves writes
void updateKeys(IKVClerk* ck, map<string, pair<string, uint64_t>> &kvs, string tag){
    for(auto &[key, vv] : kvs){
        string value = tag + "-" + key;
        check(ck->put(key, value, vv.second) == OK, "update " + key);
        vv = { value, vv.second + 1 };
    }
}


// ---------- 5A: moving shards ----------

void testInitQuery5A(){
    ShardTest t(true);
    ShardConfig cfg = t.query();
    check(cfg.num == 1, "first config should be num 1");
    for(int gid : cfg.shards){
        check(gid == GID1, "every shard should start in group 1");
    }
    check(cfg == ShardConfig::initial(t.groups[GID1]->serverNames()), "query doesn't match the initial config");
}


//With shards split across two groups, shutting one group down should only block keys in its shards
void testStaticShards5A(){
    ShardTest t(true);
    t.join({ 2 });

    auto c = t.makeClerk();
    auto kvs = putKeys(c->ck.get(), 20);
    checkKeys(c->ck.get(), kvs);

    ShardConfig cfg = t.query();
    t.groups[2]->shutdown();

    //Every key gets its own client, and only group 1's keys should finish
    atomic<int> done{0};
    int expected = 0;
    vector<thread> getters;
    vector<unique_ptr<ShardClient>> getter_clients;
    for(auto &[key, vv] : kvs){
        if(cfg.shards[key2shard(key)] == GID1){
            expected ++;
        }
        getter_clients.push_back(t.makeClerk());
        ShardClerk* ck = getter_clients.back()->ck.get();
        string k = key;
        string want = vv.first;
        getters.emplace_back([ck, k, want, &done](){
            check(ck->get(k).value == want, "wrong value for " + k);
            done ++;
        });
    }

    sleepMs(2000);
    check(done == expected, to_string(done) + " gets finished with group 2 down, expected " + to_string(expected));

    //Bring group 2 back, everything should finish
    t.groups[2]->startServers();
    for(thread &g : getters){
        g.join();
    }
    check(done == kvs.size(), "not every get finished after group 2 came back");
}


//Data has to survive moving to a new group
void testJoinBasic5A(){
    ShardTest t(true);
    auto c = t.makeClerk();
    auto kvs = putKeys(c->ck.get(), 20);

    t.join({ 2 });
    ShardConfig cfg = t.query();
    check(cfg.num == 2, "config should be num 2 after join");

    int in_g2 = 0;
    for(int gid : cfg.shards){
        in_g2 += gid == 2;
    }
    check(in_g2 == NSHARDS / 2, "group 2 should own half the shards, owns " + to_string(in_g2));

    checkKeys(c->ck.get(), kvs);
    updateKeys(c->ck.get(), kvs, "after-join");
    checkKeys(c->ck.get(), kvs);
}


//After a shard moves away, the old group should actually throw its data out
void testDeleteBasic5A(){
    int max_raft_state = 1000;
    ShardTest t(true, max_raft_state);
    auto c = t.makeClerk();

    //Big values so the shard data dominates the snapshot
    string big(1000, 'x');
    map<string, pair<string, uint64_t>> kvs;
    for(int i = 0; i < 30; i ++){
        string key = "k" + to_string(i);
        check(c->ck->put(key, big + to_string(i), 0) == OK, "put " + key);
        kvs[key] = { big + to_string(i), 1 };
    }
    int before = t.groups[GID1]->snapshotSize();
    check(before > 15000, "group 1 should have snapshotted its data (" + to_string(before) + ")");

    t.join({ 2 });

    //Do some small writes on group 1's remaining keys so it takes a fresh snapshot
    ShardConfig cfg = t.query();
    for(int round = 0; round < 3; round ++){
        for(auto &[key, vv] : kvs){
            if(cfg.shards[key2shard(key)] == GID1){
                string value = big + "-" + to_string(round);
                check(c->ck->put(key, value, vv.second) == OK, "update " + key);
                vv = { value, vv.second + 1 };
            }
        }
    }

    int after = t.groups[GID1]->snapshotSize();
    check(after < before * 3 / 4, "group 1 didn't delete moved shards (snapshot " + to_string(before) +
          " -> " + to_string(after) + ")");
    checkKeys(c->ck.get(), kvs);
}


//Group 1 hands everything to group 2 and goes away for good
void testJoinLeaveBasic5A(){
    ShardTest t(true);
    auto c = t.makeClerk();
    auto kvs = putKeys(c->ck.get(), 20);

    t.join({ 2 });
    checkKeys(c->ck.get(), kvs);

    t.leave({ GID1 });
    ShardConfig cfg = t.query();
    for(int gid : cfg.shards){
        check(gid == 2, "every shard should be in group 2 after group 1 leaves");
    }

    t.groups[GID1]->shutdown();
    checkKeys(c->ck.get(), kvs);
    updateKeys(c->ck.get(), kvs, "g2-only");
    checkKeys(c->ck.get(), kvs);
}


void testManyJoinLeave5A(){
    ShardTest t(true);
    auto c = t.makeClerk();
    auto kvs = putKeys(c->ck.get(), 20);

    vector<int> extra = { 2, 3 };
    for(int i = 0; i < 6; i ++){
        int gid = extra[i % 2];
        if(i % 4 < 2){
            t.join({ gid });
        }
        else {
            t.leave({ gid });
        }
        checkKeys(c->ck.get(), kvs);
        updateKeys(c->ck.get(), kvs, "round" + to_string(i));
    }
    checkKeys(c->ck.get(), kvs);
}


//Groups that have left shouldn't be needed anymore
void testShutdown5A(){
    ShardTest t(true);
    auto c = t.makeClerk();
    auto kvs = putKeys(c->ck.get(), 20);

    t.join({ 2, 3 });
    checkKeys(c->ck.get(), kvs);

    t.leave({ GID1, 2 });
    t.groups[GID1]->shutdown();
    t.groups[2]->shutdown();

    checkKeys(c->ck.get(), kvs);
    updateKeys(c->ck.get(), kvs, "g3");
    checkKeys(c->ck.get(), kvs);
}


//Clients hammer random keys while the controller keeps joining/leaving groups. Every op is recorded
//and the whole history has to be linearizable.
//reconfigure(t, round) does one config change - lets 5B/5C plug in crashing/concurrent controllers
void concurrentClerks(int n_clients, bool reliable, function<void(ShardTest &, int)> reconfigure, int rounds = 8){
    ShardTest t(reliable);

    atomic<bool> stop{false};
    vector<int> puts(n_clients, 0);
    vector<thread> clients;
    for(int c = 0; c < n_clients; c ++){
        clients.emplace_back([&, c](){
            auto client = t.makeClerk();
            RecordingClerk rck(client->ck.get(), &t.history);
            int n = 0;
            while(!stop){
                string key = "k" + to_string(randInt(0, 12));
                GetResult r = rck.get(key);
                string err = rck.put(key, "c" + to_string(c) + "-" + to_string(n ++), r.err == ErrNoKey ? 0 : r.version);
                if(err == OK){
                    puts[c] ++;
                }
            }
        });
    }

    for(int round = 0; round < rounds; round ++){
        reconfigure(t, round);
        sleepMs(200);
    }
    stop = true;
    for(thread &c : clients){
        c.join();
    }

    int total = 0;
    for(int p : puts){
        total += p;
    }
    check(total > 0, "no put ever went through");

    //Read everything at the end so the checker sees the final state
    auto c = t.makeClerk();
    RecordingClerk rck(c->ck.get(), &t.history);
    for(int k = 0; k < 12; k ++){
        rck.get("k" + to_string(k));
    }

    t.checkLinearizable();
}


//Join/leave groups 2 and 3 in a fixed cycle
void joinLeaveCycle(ShardTest &t, int round){
    switch(round % 4){
        case 0: t.join({ 2 }); break;
        case 1: t.join({ 3 }); break;
        case 2: t.leave({ 2 }); break;
        case 3: t.leave({ 3 }); break;
    }
}


// ---------- 5B: controller crashes ----------

//The real 5B scenario - the controller dies at a random point mid-change
void testJoinLeave5B(){
    ShardTest t(true);
    auto c = t.makeClerk();
    auto kvs = putKeys(c->ck.get(), 20);

    for(int round = 0; round < 6; round ++){
        int before = t.query().num;
        auto doomed = t.makeController();

        //Figure out the change up front (both controllers would compute the same thing anyway)
        int gid = round % 2 == 0 ? 2 : 3;
        t.addGroup(gid);
        ShardConfig cur = t.query();
        ShardConfig next = (round % 4 < 2) ? cur.join({ { gid, t.groups[gid]->serverNames() } }) : cur.leave({ gid });

        Controller* d = doomed.get();
        thread change([d, next](){ d->sck->changeConfigTo(next); });
        sleepMs(randInt(0, 40));
        d->sck->kill();
        change.join();

        //A new controller comes up and finishes whatever was left
        auto fresh = t.makeController();
        fresh->sck->initController();
        if(fresh->sck->query().num == before){
            fresh->sck->changeConfigTo(next);   //the doomed one died before even claiming it
        }

        check(t.query().num == before + 1, "config should be " + to_string(before + 1) + " after recovery, is " +
              to_string(t.query().num));
        checkKeys(c->ck.get(), kvs);
        updateKeys(c->ck.get(), kvs, "r" + to_string(round));
    }
}


//Same thing with clients running the whole time
void testRecoverCtrler5B(){
    concurrentClerks(3, true, [](ShardTest &t, int round){
        int before = t.query().num;
        int gid = round % 2 == 0 ? 2 : 3;
        t.addGroup(gid);
        ShardConfig cur = t.query();
        ShardConfig next = (round % 4 < 2) ? cur.join({ { gid, t.groups[gid]->serverNames() } }) : cur.leave({ gid });

        auto doomed = t.makeController();
        Controller* d = doomed.get();
        thread change([d, next](){ d->sck->changeConfigTo(next); });
        sleepMs(randInt(0, 40));
        d->sck->kill();
        change.join();

        auto fresh = t.makeController();
        fresh->sck->initController();
        if(fresh->sck->query().num == before){
            fresh->sck->changeConfigTo(next);
        }
        check(t.query().num == before + 1, "config didn't advance after recovery");
    });
}


// ---------- 5C: concurrent controllers ----------

//Several controllers all try to make the same change at once - it has to happen exactly once
void concurrentCtrlersChange(ShardTest &t, int round){
    int before = t.query().num;
    int gid = round % 2 == 0 ? 2 : 3;
    t.addGroup(gid);
    ShardConfig cur = t.query();
    ShardConfig next = (round % 4 < 2) ? cur.join({ { gid, t.groups[gid]->serverNames() } }) : cur.leave({ gid });

    vector<unique_ptr<Controller>> ctrls;
    for(int i = 0; i < 3; i ++){
        ctrls.push_back(t.makeController());
    }
    vector<thread> threads;
    for(auto &c : ctrls){
        Controller* cp = c.get();
        threads.emplace_back([cp, next](){ cp->sck->changeConfigTo(next); });
    }
    for(thread &th : threads){
        th.join();
    }

    ShardConfig after = t.query();
    check(after.num == before + 1, "config should advance by exactly one, went " + to_string(before) + " -> " + to_string(after.num));
    check(after == next, "final config isn't the one the controllers agreed on");
}


void testConcurrentCtrlers5C(){
    ShardTest t(true);
    auto c = t.makeClerk();
    auto kvs = putKeys(c->ck.get(), 20);

    for(int round = 0; round < 6; round ++){
        concurrentCtrlersChange(t, round);
        checkKeys(c->ck.get(), kvs);
        updateKeys(c->ck.get(), kvs, "r" + to_string(round));
    }
}


//A controller gets cut off from the shard groups mid-change and hangs there. Another controller finishes
//the change and makes another one. When the old controller's network comes back it carries on with its
//stale moves - which must not break anything.
void testPartitionCtrler5C(){
    ShardTest t(true);
    auto c = t.makeClerk();
    auto kvs = putKeys(c->ck.get(), 20);

    t.addGroup(2);
    ShardConfig cur = t.query();
    ShardConfig next = cur.join({ { 2, t.groups[2]->serverNames() } });

    auto stale = t.makeController();
    Controller* sp = stale.get();
    thread change([sp, next](){ sp->sck->changeConfigTo(next); });
    sleepMs(randInt(5, 30));
    t.clients.connectClientTo(stale->client_id, t.ctrlServerNames());   //can still reach its kv, not the groups

    //Someone else finishes it, then moves on to the config after
    auto fresh = t.makeController();
    fresh->sck->initController();
    if(fresh->sck->query().num == cur.num){
        fresh->sck->changeConfigTo(next);
    }
    check(t.query().num == next.num, "fresh controller didn't finish the change");
    checkKeys(c->ck.get(), kvs);

    t.join({ 3 }, fresh.get());
    check(t.query().num == next.num + 1, "second change didn't go through");
    updateKeys(c->ck.get(), kvs, "after-3");

    //Let the stale controller loose
    t.clients.connectClientAll(stale->client_id);
    change.join();

    check(t.query().num == next.num + 1, "stale controller changed the config");
    checkKeys(c->ck.get(), kvs);
    updateKeys(c->ck.get(), kvs, "after-stale");
    checkKeys(c->ck.get(), kvs);
}


int main(int argc, char** argv){
    return runTests({
        { "TestInitQuery5A", testInitQuery5A },
        { "TestStaticShards5A", testStaticShards5A },
        { "TestJoinBasic5A", testJoinBasic5A },
        { "TestDeleteBasic5A", testDeleteBasic5A },
        { "TestJoinLeaveBasic5A", testJoinLeaveBasic5A },
        { "TestManyJoinLeave5A", testManyJoinLeave5A },
        { "TestShutdown5A", testShutdown5A },
        { "TestOneConcurrentClerkReliable5A", [](){ concurrentClerks(1, true, joinLeaveCycle); } },
        { "TestManyConcurrentClerkReliable5A", [](){ concurrentClerks(5, true, joinLeaveCycle); } },
        { "TestOneConcurrentClerkUnreliable5A", [](){ concurrentClerks(1, false, joinLeaveCycle); } },
        { "TestManyConcurrentClerkUnreliable5A", [](){ concurrentClerks(5, false, joinLeaveCycle); } },

        { "TestJoinLeave5B", testJoinLeave5B },
        { "TestRecoverCtrler5B", testRecoverCtrler5B },

        { "TestConcurrentCtrlers5C", testConcurrentCtrlers5C },
        { "TestConcurrentCtrlersClerks5C", [](){ concurrentClerks(3, true, concurrentCtrlersChange); } },
        { "TestPartitionCtrler5C", testPartitionCtrler5C },
    }, argc, argv);
}
