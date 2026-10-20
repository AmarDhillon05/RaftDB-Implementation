#pragma once

#include <vector>
#include <string>
#include <map>
#include <algorithm>
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::json;


//Shard configuration - which replica group owns which shard. Keys are split into NSHARDS shards by
//hash, and every config says, for every shard, which group (gid) serves it, plus the server names
//of each group. Configs are numbered and only ever move forward.


const int NSHARDS = 12;
const int GID1 = 1;     //the first group - owns every shard in the very first config


//Which shard a key lives in (FNV-1a, same as ihash in map reduce)
int key2shard(const string &key){
    unsigned int hash = 2166136261u;
    for(char c : key){
        hash ^= (unsigned char) c;
        hash *= 16777619u;
    }
    return hash % NSHARDS;
}


struct ShardConfig {
    int num = 0;
    vector<int> shards = vector<int>(NSHARDS, 0);    //shard -> gid (0 = nobody)
    map<int, vector<string>> groups;                  //gid -> server names


    json toJson() const {
        json j;
        j["num"] = num;
        j["shards"] = shards;
        json g = json::object();
        for(auto &[gid, servers] : groups){
            g[to_string(gid)] = servers;
        }
        j["groups"] = g;
        return j;
    }


    static ShardConfig fromJson(const json &j){
        ShardConfig c;
        c.num = j["num"];
        c.shards = j["shards"].get<vector<int>>();
        for(auto &[gid, servers] : j["groups"].items()){
            c.groups[stoi(gid)] = servers.get<vector<string>>();
        }
        return c;
    }


    //The first config - group 1 owns everything
    static ShardConfig initial(vector<string> gid1_servers){
        ShardConfig c;
        c.num = 1;
        c.shards = vector<int>(NSHARDS, GID1);
        c.groups[GID1] = gid1_servers;
        return c;
    }


    //Next config with these groups added, shards rebalanced
    ShardConfig join(map<int, vector<string>> new_groups) const {
        ShardConfig next = *this;
        next.num = num + 1;
        for(auto &[gid, servers] : new_groups){
            next.groups[gid] = servers;
        }
        next.rebalance();
        return next;
    }


    //Next config with these groups removed, their shards handed to whoever's left
    ShardConfig leave(vector<int> gids) const {
        ShardConfig next = *this;
        next.num = num + 1;
        for(int gid : gids){
            next.groups.erase(gid);
        }
        next.rebalance();
        return next;
    }


    //Spread shards as evenly as possible while moving as few as possible: anything owned by a group
    //that's gone, or by a group over its fair share, goes into a pool, and the pool fills up whoever
    //is under their share. (Deterministic, so every controller computes the same thing.)
    void rebalance(){
        if(groups.empty()){
            shards = vector<int>(NSHARDS, 0);
            return;
        }

        vector<int> gids;
        for(auto &[gid, servers] : groups){
            gids.push_back(gid);
        }

        //Fair share - the first (NSHARDS % n) groups get one extra
        map<int, int> target;
        for(int i = 0; i < gids.size(); i ++){
            target[gids[i]] = NSHARDS / gids.size() + (i < NSHARDS % gids.size() ? 1 : 0);
        }

        map<int, int> count;
        vector<int> pool;
        for(int s = 0; s < NSHARDS; s ++){
            int gid = shards[s];
            if(!groups.count(gid) || count[gid] >= target[gid]){
                pool.push_back(s);
            }
            else {
                count[gid] ++;
            }
        }

        for(int gid : gids){
            while(count[gid] < target[gid] && !pool.empty()){
                shards[pool.back()] = gid;
                pool.pop_back();
                count[gid] ++;
            }
        }
    }


    bool operator==(const ShardConfig &o) const {
        return num == o.num && shards == o.shards && groups == o.groups;
    }

};
