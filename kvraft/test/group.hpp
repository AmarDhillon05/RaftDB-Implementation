#pragma once

#include <vector>
#include <string>
#include <map>
#include <set>
#include <memory>
#include <mutex>
#include <functional>
#include "../../common/testing.hpp"
#include "../../common/labrpc.hpp"
#include "../../raft/raft.hpp"

using namespace std;


//Test harness for a replica group - n servers running some raft-backed service on the fake network.
//Lab 4 uses one group, lab 5 uses several (plus the controller's group). The service is made by a
//factory so the same harness works for the rsm tests, the kv server, and the shard groups.


//What the harness needs to know about one running server
struct ServiceHandle {
    shared_ptr<void> service;       //keeps the service alive
    shared_ptr<Server> server;      //its RPC handlers (raft's + the service's)
    shared_ptr<Raft> raft;
    function<void()> kill;
};

typedef function<ServiceHandle(vector<shared_ptr<ClientEnd>> peers, int me, shared_ptr<Persister> persister,
                               int max_raft_state)> ServiceFactory;


const int ELECTION_TIMEOUT_MS = 1000;


class Group {

    public:

        Group(shared_ptr<Network> net, int gid, int n, int max_raft_state, ServiceFactory factory){
            this->net = net;
            this->gid = gid;
            this->n = n;
            this->max_raft_state = max_raft_state;
            this->factory = factory;

            services.resize(n);
            saved.resize(n);
            alive.resize(n, false);
            part.resize(n, 0);
            endnames.resize(n, vector<string>(n));

            startServers();
        }


        ~Group(){
            shutdown();
        }


        int size(){
            return n;
        }


        string serverName(int i){
            return "server-" + to_string(gid) + "-" + to_string(i);
        }


        vector<string> serverNames(){
            vector<string> names;
            for(int i = 0; i < n; i ++){
                names.push_back(serverName(i));
            }
            return names;
        }


        //(Re)start server i from what it persisted
        void startServer(int i){
            shutdownServer(i);

            vector<shared_ptr<ClientEnd>> ends;
            for(int j = 0; j < n; j ++){
                endnames[i][j] = serverName(i) + "->" + to_string(j) + "-" + to_string(randInt(0, 1 << 30));
                ends.push_back(net->makeEnd(endnames[i][j]));
                net->connect(endnames[i][j], serverName(j));
            }

            saved[i] = saved[i] ? saved[i]->copy() : make_shared<Persister>();
            ServiceHandle handle = factory(ends, i, saved[i], max_raft_state);
            {
                lock_guard<mutex> lock(mu);
                services[i] = handle;
                alive[i] = true;
            }
            net->addServer(serverName(i), handle.server);
            updateLinks();
        }


        //Crash server i - it stops answering, but its persisted state is kept for startServer
        void shutdownServer(int i){
            ServiceHandle handle;
            {
                lock_guard<mutex> lock(mu);
                if(!alive[i]){
                    return;
                }
                alive[i] = false;
                handle = services[i];
                services[i] = ServiceHandle();
            }
            updateLinks();
            net->deleteServer(serverName(i));
            handle.kill();

            //Copy so the dead instance can't keep writing to what the next instance will read
            saved[i] = saved[i]->copy();
        }


        void startServers(){
            for(int i = 0; i < n; i ++){
                startServer(i);
            }
        }


        void shutdown(){
            for(int i = 0; i < n; i ++){
                shutdownServer(i);
            }
        }


        //Servers in p1 can talk to each other, servers in p2 can talk to each other, nothing crosses
        void partition(vector<int> p1, vector<int> p2){
            for(int i : p1){
                part[i] = 1;
            }
            for(int i : p2){
                part[i] = 2;
            }
            updateLinks();
        }


        void connectAll(){
            for(int i = 0; i < n; i ++){
                part[i] = 0;
            }
            updateLinks();
        }


        //Random split into two halves - the first one has the majority
        pair<vector<int>, vector<int>> randomPartition(){
            vector<int> a, b;
            for(int i = 0; i < n; i ++){
                (randInt(0, 2) == 0 ? a : b).push_back(i);
            }
            return { a, b };
        }


        //Server i alone vs everyone else
        pair<vector<int>, vector<int>> isolate(int i){
            vector<int> rest;
            for(int j = 0; j < n; j ++){
                if(j != i){
                    rest.push_back(j);
                }
            }
            return { rest, { i } };
        }


        ServiceHandle service(int i){
            lock_guard<mutex> lock(mu);
            return services[i];
        }


        //Index of a server that thinks it's leader, or -1
        int leader(){
            for(int i = 0; i < n; i ++){
                ServiceHandle s = service(i);
                if(s.raft && s.raft->getState().second){
                    return i;
                }
            }
            return -1;
        }


        //Keeps looking for a leader for a while
        int waitForLeader(){
            for(int iters = 0; iters < 50; iters ++){
                int l = leader();
                if(l != -1){
                    return l;
                }
                sleepMs(100);
            }
            check(false, "no leader in group " + to_string(gid));
            return -1;
        }


        int logSize(){
            int biggest = 0;
            for(int i = 0; i < n; i ++){
                if(saved[i]){
                    biggest = max(biggest, saved[i]->raftStateSize());
                }
            }
            return biggest;
        }


        int snapshotSize(){
            int biggest = 0;
            for(int i = 0; i < n; i ++){
                if(saved[i]){
                    biggest = max(biggest, saved[i]->snapshotSize());
                }
            }
            return biggest;
        }


    private:

        mutex mu;
        shared_ptr<Network> net;
        int gid;
        int n;
        int max_raft_state;
        ServiceFactory factory;
        vector<ServiceHandle> services;
        vector<shared_ptr<Persister>> saved;
        vector<bool> alive;
        vector<int> part;                    //which side of a partition each server is on
        vector<vector<string>> endnames;     //endnames[i][j] = end server i uses to call j


        //A link i->j is up iff both are alive and on the same side of the partition
        void updateLinks(){
            lock_guard<mutex> lock(mu);
            for(int i = 0; i < n; i ++){
                for(int j = 0; j < n; j ++){
                    if(!endnames[i][j].empty()){
                        net->enable(endnames[i][j], alive[i] && alive[j] && part[i] == part[j]);
                    }
                }
            }
        }

};


//Hands out client ends and lets tests choose which servers each client can reach
class ClientNet {

    public:

        ClientNet(shared_ptr<Network> net){
            this->net = net;
        }


        //Makes an end for a client to talk to servername (lab 5 clerks make these on demand)
        shared_ptr<ClientEnd> makeEnd(int client, string servername){
            lock_guard<mutex> lock(mu);
            string endname = "client-" + to_string(client) + "->" + servername + "-" + to_string(randInt(0, 1 << 30));
            auto end = net->makeEnd(endname);
            net->connect(endname, servername);
            ends[client].push_back({ endname, servername });

            bool allowed = !only.count(client) || only[client].count(servername);
            net->enable(endname, allowed);
            return end;
        }


        int newClient(){
            lock_guard<mutex> lock(mu);
            return next_client ++;
        }


        //Client can only reach the servers in `to`
        void connectClientTo(int client, vector<string> to){
            lock_guard<mutex> lock(mu);
            only[client] = set<string>(to.begin(), to.end());
            for(auto &[endname, servername] : ends[client]){
                net->enable(endname, only[client].count(servername) > 0);
            }
        }


        //Client can reach everything again
        void connectClientAll(int client){
            lock_guard<mutex> lock(mu);
            only.erase(client);
            for(auto &[endname, servername] : ends[client]){
                net->enable(endname, true);
            }
        }


    private:

        mutex mu;
        shared_ptr<Network> net;
        int next_client = 0;
        map<int, vector<pair<string, string>>> ends;    //client -> (endname, servername)
        map<int, set<string>> only;                     //client -> the only servers it may reach
};
