#pragma once

#include <string>
#include <map>
#include <memory>
#include "../common/kv.hpp"
#include "shardcfg.hpp"
#include "shardctrler.hpp"
#include "shardgrp/client.hpp"

using namespace std;


//Client for the sharded kv. key -> shard -> group (from the latest config it knows) -> that group's
//leader. If the group says ErrWrongGroup (the shard moved, or is mid-move), go ask the controller
//for the latest config and try again.


class ShardClerk : public IKVClerk {

    public:

        ShardClerk(ShardCtrler* sck, MakeEnd make_end){
            this->sck = sck;
            this->make_end = make_end;
        }


        GetResult get(string key) override {
            json args;
            args["key"] = key;
            bool ambiguous = false;

            json reply = callOwner(key, "ShardGrp.Get", args, ambiguous);
            GetResult result;
            result.err = reply["err"];
            result.value = result.err == OK ? reply["value"].get<string>() : "";
            result.version = result.err == OK ? reply["version"].get<uint64_t>() : 0;
            return result;
        }


        //Same ErrMaybe rule as before, tracked across groups too - if the put might have applied at the
        //old owner before the shard moved, the data (with the bumped version) moved with it
        string put(string key, string value, uint64_t version) override {
            json args;
            args["key"] = key;
            args["value"] = value;
            args["version"] = version;
            bool ambiguous = false;

            json reply = callOwner(key, "ShardGrp.Put", args, ambiguous);
            string err = reply["err"];
            if(err == ErrVersion && ambiguous){
                return ErrMaybe;
            }
            return err;
        }


    private:

        ShardCtrler* sck;
        MakeEnd make_end;
        ShardConfig config;
        map<int, unique_ptr<GroupClerk>> clerks;


        //Keep sending to whichever group owns key until one actually handles it
        json callOwner(string key, string method, json args, bool &ambiguous){
            int shard = key2shard(key);

            while(true){
                if(config.num == 0){
                    config = sck->query();
                }

                int gid = config.shards[shard];
                if(gid != 0 && config.groups.count(gid)){
                    if(!clerks.count(gid)){
                        clerks[gid] = make_unique<GroupClerk>(config.groups[gid], make_end);
                    }

                    json reply = clerks[gid]->call(method, args, 3, ambiguous);
                    if(reply["err"] != ErrWrongGroup){
                        return reply;
                    }
                }

                //Wrong group / unreachable - the config must have moved on
                sleepMs(100);
                config = sck->query();
            }
        }

};
