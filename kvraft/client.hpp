#pragma once

#include <string>
#include <vector>
#include <memory>
#include <functional>
#include "../common/labrpc.hpp"
#include "../common/kv.hpp"

using namespace std;


//Client for the raft kv. Same retry logic as the lab 2 clerk, plus finding the leader: try servers in
//turn until one doesn't say ErrWrongLeader, and remember it for next time.


class Clerk : public IKVClerk {

    public:

        Clerk(vector<shared_ptr<ClientEnd>> servers){
            this->servers = servers;
        }


        GetResult get(string key) override {
            json args;
            args["key"] = key;

            while(true){
                json reply;
                if(servers[leader]->call("KVServer.Get", args, reply) && reply["err"] != ErrWrongLeader){
                    GetResult result;
                    result.err = reply["err"];
                    result.value = result.err == OK ? reply["value"].get<string>() : "";
                    result.version = result.err == OK ? reply["version"].get<uint64_t>() : 0;
                    return result;
                }
                nextServer();
            }
        }


        //ErrMaybe logic from lab 2 - but now *any* failed attempt counts as "might have applied",
        //including ErrWrongLeader, since a leader can append our put, lose leadership, and the next
        //leader can still commit it
        string put(string key, string value, uint64_t version) override {
            json args;
            args["key"] = key;
            args["value"] = value;
            args["version"] = version;

            bool first_try = true;
            while(true){
                json reply;
                if(servers[leader]->call("KVServer.Put", args, reply) && reply["err"] != ErrWrongLeader){
                    string err = reply["err"];
                    if(err == ErrVersion && !first_try){
                        return ErrMaybe;
                    }
                    return err;
                }
                first_try = false;
                nextServer();
            }
        }


    private:

        vector<shared_ptr<ClientEnd>> servers;
        int leader = 0;
        int tries = 0;


        //Move on to the next server, pausing after a full lap so we don't spin during an election
        void nextServer(){
            leader = (leader + 1) % servers.size();
            tries ++;
            if(tries % servers.size() == 0){
                sleepMs(100);
            }
        }

};
