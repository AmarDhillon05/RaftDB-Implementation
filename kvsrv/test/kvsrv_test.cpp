#include <vector>
#include <thread>
#include <atomic>
#include <memory>
#include "../../common/testing.hpp"
#include "../server.hpp"
#include "../client.hpp"
#include "../lock.hpp"

using namespace std;


//Test setup - one kv server on a fake network, plus helpers to make clients
struct KVTest {
    shared_ptr<Network> net = make_shared<Network>();
    KVServer kv;
    atomic<int> n_clerks{0};

    KVTest(bool reliable){
        net->addServer("server", kv.makeServer());
        net->setReliable(reliable);
    }

    unique_ptr<Clerk> makeClerk(){
        string name = "client" + to_string(n_clerks ++);
        auto end = net->makeEnd(name);
        net->connect(name, "server");
        net->enable(name, true);
        return make_unique<Clerk>(end);
    }
};


//Basic put/get + every error case, one client, no failures
void testReliablePut(){
    KVTest t(true);
    auto ck = t.makeClerk();

    check(ck->put("k", "v1", 0) == OK, "first put should create the key");

    GetResult r = ck->get("k");
    check(r.err == OK && r.value == "v1" && r.version == 1, "get after first put");

    check(ck->put("k", "nope", 0) == ErrVersion, "put with an old version should be ErrVersion");
    check(ck->put("k", "v2", 1) == OK, "put with the right version");

    r = ck->get("k");
    check(r.value == "v2" && r.version == 2, "get after second put");

    check(ck->put("y", "v", 1) == ErrNoKey, "put to a missing key with version != 0");
    check(ck->get("y").err == ErrNoKey, "get of a missing key");
}


//Several clients doing read-then-put on one key at the same time. Exactly one put per version can
//win, so the final version has to line up with how many puts said OK (+ some of the ErrMaybes)
void testPutConcurrent(bool reliable){
    KVTest t(reliable);
    auto setup = t.makeClerk();
    check(setup->put("k", "", 0) == OK, "setup put");

    int n_clients = 5;
    atomic<int> oks{0};
    atomic<int> maybes{0};
    atomic<bool> stop{false};
    vector<thread> clients;

    for(int c = 0; c < n_clients; c ++){
        clients.emplace_back([&, c](){
            auto ck = t.makeClerk();
            uint64_t last_seen = 0;

            while(!stop){
                GetResult r = ck->get("k");
                check(r.version >= last_seen, "versions should never go backwards for one client");
                last_seen = r.version;

                string err = ck->put("k", to_string(c), r.version);
                if(err == OK){
                    oks ++;
                }
                else if(err == ErrMaybe){
                    maybes ++;
                }
            }
        });
    }

    sleepMs(reliable ? 1000 : 3000);
    stop = true;
    for(thread &c : clients){
        c.join();
    }

    uint64_t final_version = setup->get("k").version;
    check(final_version >= 1 + oks, "lost an OK put");
    check(final_version <= 1 + oks + maybes, "more puts applied than could have");
    if(reliable){
        check(maybes == 0, "no ErrMaybe on a reliable network");
    }
    check(oks > 0, "no put ever succeeded");
}


//One client on a lossy network - since nobody else writes, ErrMaybe must mean our put went through
void testUnreliableNet(){
    KVTest t(false);
    auto ck = t.makeClerk();
    uint64_t version = 0;

    for(int i = 0; i < 100; i ++){
        string value = "v" + to_string(i);
        string err = ck->put("k", value, version);
        check(err == OK || err == ErrMaybe, "put should be OK or ErrMaybe, got " + err);

        GetResult r = ck->get("k");
        check(r.value == value && r.version == version + 1, "single client put didn't apply");
        version = r.version;
    }
}


//n clients fight over one lock. Inside the critical section a shared "holder" has to be empty when we
//get in and still be us when we leave - if two clients ever overlap, one of the checks trips
void testLock(int n_clients, bool reliable){
    KVTest t(reliable);
    atomic<int> holder{-1};
    atomic<int> acquisitions{0};
    atomic<bool> stop{false};
    vector<thread> clients;

    for(int c = 0; c < n_clients; c ++){
        clients.emplace_back([&, c](){
            auto ck = t.makeClerk();
            Lock lock(ck.get(), "lock");

            while(!stop){
                lock.acquire();

                check(holder == -1, "two clients hold the lock at once");
                holder = c;
                sleepMs(1);
                check(holder == c, "someone else got in while we held the lock");
                holder = -1;
                acquisitions ++;

                lock.release();
            }
        });
    }

    sleepMs(reliable ? 1000 : 3000);
    stop = true;
    for(thread &c : clients){
        c.join();
    }
    check(acquisitions > 0, "nobody ever got the lock");
}


int main(int argc, char** argv){
    return runTests({
        { "TestReliablePut", testReliablePut },
        { "TestPutConcurrentReliable", [](){ testPutConcurrent(true); } },
        { "TestUnreliableNet", testUnreliableNet },
        { "TestPutConcurrentUnreliable", [](){ testPutConcurrent(false); } },
        { "TestOneClientReliable", [](){ testLock(1, true); } },
        { "TestManyClientsReliable", [](){ testLock(10, true); } },
        { "TestOneClientUnreliable", [](){ testLock(1, false); } },
        { "TestManyClientsUnreliable", [](){ testLock(10, false); } },
    }, argc, argv);
}
