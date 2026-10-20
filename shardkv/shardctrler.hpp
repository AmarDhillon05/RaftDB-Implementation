#pragma once

#include <string>
#include <map>
#include <memory>
#include <atomic>
#include "../common/kv.hpp"
#include "shardcfg.hpp"
#include "shardgrp/client.hpp"

using namespace std;


//Shard controller - decides config changes and actually moves the shards. It keeps its state in a
//fault tolerant kv (the lab 4 raft kv) under two keys:
//
//  "config" -> the current config (what clients route by)
//  "next"   -> the config being moved to right now, if a change is in progress
//
//A change to config N+1 goes:
//  1. claim it - versioned put of N+1 into "next" (if two controllers race, only one put can win)
//  2. for every shard that changes owner: freeze at the old group, install at the new one, delete at the old one
//  3. versioned put of N+1 into "config" - clients start using it
//
//If the controller dies anywhere in there, "next" is still sitting in the kv, so the next controller
//(initController) sees next > config and redoes steps 2 + 3. Every shard RPC is tagged with N+1 and the
//groups ignore anything older than what they've seen, so redoing moves is harmless.


class ShardCtrler {

    public:

        ShardCtrler(IKVClerk* kv, MakeEnd make_end){
            this->kv = kv;
            this->make_end = make_end;
        }


        //Store the very first config (only the first call does anything)
        void initConfig(ShardConfig cfg){
            kv->put("config", cfg.toJson().dump(), 0);
        }


        //Call when a controller starts up - finishes any change a crashed controller left half done
        void initController(){
            ShardConfig cur = query();
            auto [next, next_version] = readNext();
            if(next.num > cur.num){
                finishChange(cur, next);
            }
        }


        ShardConfig query(){
            GetResult r = kv->get("config");
            if(r.err != OK){
                return ShardConfig();
            }
            return ShardConfig::fromJson(json::parse(r.value));
        }


        //Move from the current config to next (next.num has to be current + 1). If another controller is
        //already in the middle of a change, help it finish instead.
        void changeConfigTo(ShardConfig next){
            ShardConfig cur = query();
            if(next.num != cur.num + 1){
                return;   //stale - someone already moved past this
            }

            //Step 1: claim the change
            auto [pending, version] = readNext();
            if(pending.num > cur.num){
                finishChange(cur, pending);   //someone else's change in flight - finish theirs
                return;
            }

            string err = kv->put("next", next.toJson().dump(), version);
            if(err != OK){
                //Lost the race (ErrVersion), or ErrMaybe - either way, read back what's there and finish it
                auto [claimed, v] = readNext();
                if(claimed.num > cur.num){
                    finishChange(cur, claimed);
                }
                return;
            }

            finishChange(cur, next);
        }


        //For tests - simulates the controller crashing partway through a change
        void kill(){
            dead = true;
        }


    private:

        IKVClerk* kv;
        MakeEnd make_end;
        atomic<bool> dead{false};
        map<int, unique_ptr<GroupClerk>> clerks;


        //Returns ("next" config, its version) - num 0 if there's never been one
        pair<ShardConfig, uint64_t> readNext(){
            GetResult r = kv->get("next");
            if(r.err != OK){
                return { ShardConfig(), 0 };
            }
            return { ShardConfig::fromJson(json::parse(r.value)), r.version };
        }


        GroupClerk* clerkFor(int gid, const vector<string> &servers){
            if(!clerks.count(gid)){
                clerks[gid] = make_unique<GroupClerk>(servers, make_end);
            }
            return clerks[gid].get();
        }


        //Steps 2 + 3 - move every shard that changes owner, then publish the new config
        void finishChange(ShardConfig cur, ShardConfig next){
            for(int shard = 0; shard < NSHARDS; shard ++){
                int from = cur.shards[shard];
                int to = next.shards[shard];
                if(from == to){
                    continue;
                }
                if(dead){
                    return;
                }

                json args;
                args["shard"] = shard;
                args["num"] = next.num;
                bool ambiguous = false;

                //Freeze at the old owner and grab the data
                json data = json::object();
                if(from != 0){
                    json reply = clerkFor(from, cur.groups[from])->call("ShardGrp.FreezeShard", args, -1, ambiguous);
                    if(reply["err"] == ErrVersion){
                        return;   //a newer config already moved this shard, so this change is long done
                    }
                    data = reply["data"];
                }
                if(dead){
                    return;
                }

                //Install at the new owner
                if(to != 0){
                    json install_args = args;
                    install_args["data"] = data;
                    clerkFor(to, next.groups[to])->call("ShardGrp.InstallShard", install_args, -1, ambiguous);
                }
                if(dead){
                    return;
                }

                //Old owner can let go of it now
                if(from != 0){
                    clerkFor(from, cur.groups[from])->call("ShardGrp.DeleteShard", args, -1, ambiguous);
                }
            }
            if(dead){
                return;
            }

            //Step 3: publish - versioned so it only happens once even if two controllers get here
            GetResult r = kv->get("config");
            if(r.err == OK && ShardConfig::fromJson(json::parse(r.value)).num == cur.num){
                kv->put("config", next.toJson().dump(), r.version);
            }
        }

};
