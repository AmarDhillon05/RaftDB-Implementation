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


int main(int argc, char** argv){
    return runTests({
        { "TestInitialElection3A", testInitialElection3A },
        { "TestReElection3A", testReElection3A },
        { "TestManyElections3A", testManyElections3A },
    }, argc, argv);
}
