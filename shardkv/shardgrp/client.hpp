#pragma once

#include <string>
#include <vector>
#include <memory>
#include <functional>
#include "../../common/labrpc.hpp"
#include "../../common/kv.hpp"

using namespace std;


//Talks to one shard group - finds its leader and retries, like the lab 4 clerk. Unlike that clerk it
//gives up after a few laps of the servers (returning ErrWrongGroup), because the group might have been
//shut down after leaving, and the shard clerk should go check the latest config instead of hanging.


typedef function<shared_ptr<ClientEnd>(string servername)> MakeEnd;


class GroupClerk {

    public:

        GroupClerk(vector<string> servernames, MakeEnd make_end){
            for(string name : servernames){
                servers.push_back(make_end(name));
            }
        }


        //Sends method to the group's leader. Returns the reply, or {"err": ErrWrongGroup} if nobody
        //answered after max_laps tries of every server (max_laps = -1 means keep trying forever).
        //ambiguous gets set if some attempt might have gone through without us hearing back.
        json call(string method, json args, int max_laps, bool &ambiguous){
            for(int tries = 0; max_laps < 0 || tries < max_laps * servers.size(); tries ++){
                json reply;
                bool ok = servers[leader]->call(method, args, reply);

                if(ok && reply["err"] != ErrWrongLeader){
                    return reply;
                }

                //No reply = the request may have been applied. ErrWrongLeader can also mean it was
                //appended by a leader that then lost leadership and later still got committed.
                ambiguous = true;
                leader = (leader + 1) % servers.size();
                if((tries + 1) % servers.size() == 0){
                    sleepMs(100);
                }
            }

            json reply;
            reply["err"] = ErrWrongGroup;
            return reply;
        }


    private:

        vector<shared_ptr<ClientEnd>> servers;
        int leader = 0;

};
