#pragma once

#include <string>
#include <map>
#include <vector>
#include <memory>
#include "../../common/labrpc.hpp"
#include "../../common/kv.hpp"
#include "../../kvraft/rsm.hpp"
#include "../shardcfg.hpp"

using namespace std;


//One replica group of the sharded kv. It's the lab 4 kv server, except it only serves the shards it
//currently owns, and the controller can move a shard out of it / into it with three RPCs:
//
//  FreezeShard(shard, num)         -> stop serving the shard, hand back its data (the old owner)
//  InstallShard(shard, data, num)  -> start serving the shard with this data (the new owner)
//  DeleteShard(shard, num)         -> throw the shard's data away (the old owner, once it's installed)
//
//num is the config number the move is for. Each shard remembers the highest num it's seen, and
//anything older is ignored - so retries, duplicates and a second controller redoing the same move are
//all harmless. All of this goes through raft like everything else, so every replica agrees.


struct ShardState {
    bool owned = false;
    bool frozen = false;
    int num = 0;                                    //latest config num that touched this shard
    map<string, pair<string, uint64_t>> data;      //key -> (value, version)
};


class ShardGrp : public StateMachine, public enable_shared_from_this<ShardGrp> {

    public:

        ShardGrp(vector<shared_ptr<ClientEnd>> peers, int me, shared_ptr<Persister> persister, int max_raft_state, int gid){
            this->gid = gid;

            //Group 1 owns everything in the first config
            if(gid == GID1){
                for(ShardState &s : shards){
                    s.owned = true;
                    s.num = 1;
                }
            }

            rsm = make_unique<RSM>(peers, me, persister, max_raft_state, this);
        }


        shared_ptr<Server> makeServer(){
            auto server = rsm->raft()->makeServer();
            weak_ptr<ShardGrp> weak = shared_from_this();

            for(string op : { "Get", "Put", "FreezeShard", "InstallShard", "DeleteShard" }){
                server->addHandler("ShardGrp." + op, [weak, op](json args){
                    auto grp = weak.lock();
                    if(!grp){
                        return json();
                    }
                    args["op"] = op;
                    return grp->submitOrWrongLeader(args);
                });
            }
            return server;
        }


        shared_ptr<Raft> raft(){
            return rsm->raft();
        }


        void kill(){
            rsm->kill();
        }


        // ---------- state machine ----------

        json doOp(json req) override {
            string op = req["op"];
            json reply;

            if(op == "Get" || op == "Put"){
                ShardState &s = shards[key2shard(req["key"])];
                if(!s.owned || s.frozen){
                    reply["err"] = ErrWrongGroup;   //not ours (or mid-move) - client should refetch the config
                    return reply;
                }
                return op == "Get" ? doGet(s, req) : doPut(s, req);
            }

            ShardState &s = shards[req["shard"].get<int>()];
            int num = req["num"];

            if(op == "FreezeShard"){
                if(num < s.num){
                    reply["err"] = ErrVersion;   //a newer config already moved this shard
                    return reply;
                }
                s.num = num;
                s.frozen = true;
                reply["err"] = OK;
                reply["data"] = encodeShard(s);
                return reply;
            }

            if(op == "InstallShard"){
                //Only the first install for a config counts - a repeat (or a late one) must not overwrite
                //data that clients may have already changed since
                if(num > s.num){
                    s.num = num;
                    s.owned = true;
                    s.frozen = false;
                    decodeShard(s, req["data"]);
                }
                reply["err"] = OK;
                return reply;
            }

            if(op == "DeleteShard"){
                if(num >= s.num){
                    s.num = num;
                    s.owned = false;
                    s.frozen = false;
                    s.data.clear();
                }
                reply["err"] = OK;
                return reply;
            }

            reply["err"] = "unknown op " + op;
            return reply;
        }


        string snapshot() override {
            json snap = json::array();
            for(ShardState &s : shards){
                snap.push_back({ { "owned", s.owned }, { "frozen", s.frozen }, { "num", s.num }, { "data", encodeShard(s) } });
            }
            return snap.dump();
        }


        void restore(string snap_data) override {
            json snap = json::parse(snap_data);
            for(int i = 0; i < NSHARDS; i ++){
                shards[i].owned = snap[i]["owned"];
                shards[i].frozen = snap[i]["frozen"];
                shards[i].num = snap[i]["num"];
                decodeShard(shards[i], snap[i]["data"]);
            }
        }


    private:

        int gid;
        ShardState shards[NSHARDS];
        unique_ptr<RSM> rsm;    //declared last so it's destroyed (and its reader stopped) first


        json submitOrWrongLeader(json req){
            auto [err, result] = rsm->submit(req);
            if(err != OK){
                json reply;
                reply["err"] = ErrWrongLeader;
                return reply;
            }
            return result;
        }


        json doGet(ShardState &s, json &req){
            string key = req["key"];
            json reply;
            if(!s.data.count(key)){
                reply["err"] = ErrNoKey;
                return reply;
            }
            reply["err"] = OK;
            reply["value"] = s.data[key].first;
            reply["version"] = s.data[key].second;
            return reply;
        }


        //Same versioned put rules as labs 2 + 4
        json doPut(ShardState &s, json &req){
            string key = req["key"];
            string value = req["value"];
            uint64_t version = req["version"];
            json reply;

            if(!s.data.count(key)){
                if(version != 0){
                    reply["err"] = ErrNoKey;
                    return reply;
                }
                s.data[key] = { value, 1 };
                reply["err"] = OK;
                return reply;
            }
            if(s.data[key].second != version){
                reply["err"] = ErrVersion;
                return reply;
            }
            s.data[key] = { value, version + 1 };
            reply["err"] = OK;
            return reply;
        }


        json encodeShard(ShardState &s){
            json data = json::object();
            for(auto &[key, vv] : s.data){
                data[key] = { vv.first, vv.second };
            }
            return data;
        }


        void decodeShard(ShardState &s, const json &data){
            s.data.clear();
            for(auto &[key, vv] : data.items()){
                s.data[key] = { vv[0].get<string>(), vv[1].get<uint64_t>() };
            }
        }

};
