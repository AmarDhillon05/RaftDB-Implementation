#pragma once

#include <string>
#include <map>
#include <memory>
#include <mutex>
#include <atomic>
#include <random>
#include <thread>
#include <chrono>
#include <functional>
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::json;


//Fake in-process network for testing labs 2-5 (same idea as labrpc in the MIT labs).
//Every "machine" lives in the same process, but all calls between them go through here, so the
//tests can make the network drop messages, delay them, or cut a server off completely - which is
//basically impossible to test reliably with real sockets.
//
//  ClientEnd - one end of a connection, what a client/peer calls through
//  Server    - a set of named handlers ("Raft.RequestVote" -> function)
//  Network   - routes ClientEnd -> Server and decides what gets dropped/delayed
//
//Calls run the handler on the caller's thread. A call returning false means "no reply" - the request
//or reply got lost, or the server is down. Exactly like a real network, the caller can't tell
//whether the handler ran or not.


//Helper for random ints in [lo, hi) - one generator per thread so nothing needs a lock
int randInt(int lo, int hi){
    thread_local mt19937 rng(random_device{}());
    return lo + rng() % (hi - lo);
}


void sleepMs(int ms){
    this_thread::sleep_for(chrono::milliseconds(ms));
}


class Server {

    public:

        void addHandler(string method, function<json(json)> handler){
            handlers[method] = handler;
        }


        bool hasHandler(string method){
            return handlers.count(method) > 0;
        }


        json dispatch(string method, json args){
            count ++;
            return handlers[method](args);
        }


        int getCount(){
            return count;
        }


    private:

        map<string, function<json(json)>> handlers;
        atomic<int> count{0};

};


class Network;


class ClientEnd {

    public:

        ClientEnd(Network* net, string name){
            this->net = net;
            this->name = name;
        }

        //Send an RPC, returns true + fills reply if a reply came back
        bool call(string method, json args, json &reply);

        string name;

    private:

        Network* net;

};


class Network {

    public:

        //Make a new client end, it starts disabled + not connected to anything
        shared_ptr<ClientEnd> makeEnd(string endname){
            lock_guard<mutex> lock(mu);
            auto end = make_shared<ClientEnd>(this, endname);
            enabled[endname] = false;
            return end;
        }


        void addServer(string servername, shared_ptr<Server> server){
            lock_guard<mutex> lock(mu);
            servers[servername] = server;
        }


        //Pulls a server off the network - in flight calls to it get no reply
        void deleteServer(string servername){
            lock_guard<mutex> lock(mu);
            servers.erase(servername);
        }


        //Point a client end at a server (an end only ever talks to one server)
        void connect(string endname, string servername){
            lock_guard<mutex> lock(mu);
            connections[endname] = servername;
        }


        //Turn a client end on/off - this is how tests cut network links
        void enable(string endname, bool on){
            lock_guard<mutex> lock(mu);
            enabled[endname] = on;
        }


        //Unreliable = short random delays, and 10% of requests and 10% of replies are dropped
        void setReliable(bool on){
            reliable = on;
        }


        //Long delays = calls to a dead/disconnected server hang for a long time before failing,
        //instead of failing fast (real networks don't tell you a machine is gone)
        void setLongDelays(bool on){
            long_delays = on;
        }


        //Long reordering = replies sometimes get held back a while, so they show up out of order
        void setLongReordering(bool on){
            long_reordering = on;
        }


        int getCount(string servername){
            lock_guard<mutex> lock(mu);
            if(!servers.count(servername)){
                return 0;
            }
            return servers[servername]->getCount();
        }


        int getTotalCount(){
            return total_count;
        }


        long long getTotalBytes(){
            return total_bytes;
        }


        bool call(string endname, string method, json args, json &reply){
            total_count ++;
            total_bytes += args.dump().size();

            //Figure out where this end is pointed at the moment the request "leaves"
            shared_ptr<Server> server;
            string servername;
            bool on;
            {
                lock_guard<mutex> lock(mu);
                on = enabled[endname];
                if(connections.count(endname)){
                    servername = connections[endname];
                    if(servers.count(servername)){
                        server = servers[servername];
                    }
                }
            }

            //Dead or cut off - nobody answers, eventually the caller times out
            if(!on || !server){
                sleepMs(long_delays ? randInt(0, 7000) : randInt(0, 100));
                return false;
            }

            if(!reliable){
                sleepMs(randInt(0, 27));
                if(randInt(0, 1000) < 100){
                    return false;   //request lost
                }
            }

            if(!server->hasHandler(method)){
                return false;
            }

            json result = server->dispatch(method, args);

            //If the server got killed/replaced or cut off while the handler ran, the reply is lost
            {
                lock_guard<mutex> lock(mu);
                bool still_there = servers.count(servername) && servers[servername] == server;
                if(!still_there || !enabled[endname]){
                    return false;
                }
            }

            if(!reliable && randInt(0, 1000) < 100){
                return false;   //reply lost
            }

            if(long_reordering && randInt(0, 900) < 600){
                sleepMs(200 + randInt(0, 1 + randInt(0, 2000)));
            }

            reply = result;
            return true;
        }


    private:

        mutex mu;
        atomic<bool> reliable{true};
        atomic<bool> long_delays{false};
        atomic<bool> long_reordering{false};
        map<string, bool> enabled;
        map<string, string> connections;
        map<string, shared_ptr<Server>> servers;
        atomic<int> total_count{0};
        atomic<long long> total_bytes{0};

};


bool ClientEnd::call(string method, json args, json &reply){
    return net->call(name, method, args, reply);
}
