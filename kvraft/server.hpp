#pragma once

#include <string>
#include <map>
#include <memory>
#include "../common/labrpc.hpp"
#include "../common/kv.hpp"
#include "rsm.hpp"

using namespace std;


//Fault tolerant kv server - the same versioned Get/Put as lab 2, except every request goes through the
//raft log first (via the RSM), so all replicas apply the same requests in the same order.
//Gets go through the log too - otherwise an old leader that doesn't know it's been replaced could
//answer with stale data.
//
//The version check still does all the duplicate detection: a retried put that already applied gets
//ErrVersion, so there's no per-client table to keep in sync or put in snapshots.


class KVServer : public StateMachine, public enable_shared_from_this<KVServer> {

    public:

        KVServer(vector<shared_ptr<ClientEnd>> peers, int me, shared_ptr<Persister> persister, int max_raft_state){
            rsm = make_unique<RSM>(peers, me, persister, max_raft_state, this);
        }


        //Raft's handlers + ours on one Server, since raft peers and clients both talk to this machine
        shared_ptr<Server> makeServer(){
            auto server = rsm->raft()->makeServer();
            weak_ptr<KVServer> weak = shared_from_this();

            server->addHandler("KVServer.Get", [weak](json args){
                auto kv = weak.lock();
                return kv ? kv->get(args) : json();
            });
            server->addHandler("KVServer.Put", [weak](json args){
                auto kv = weak.lock();
                return kv ? kv->put(args) : json();
            });
            return server;
        }


        shared_ptr<Raft> raft(){
            return rsm->raft();
        }


        void kill(){
            rsm->kill();
        }


        // ---------- RPC handlers ----------

        json get(json args){
            json req;
            req["op"] = "get";
            req["key"] = args["key"];
            return submitOrWrongLeader(req);
        }


        json put(json args){
            json req;
            req["op"] = "put";
            req["key"] = args["key"];
            req["value"] = args["value"];
            req["version"] = args["version"];
            return submitOrWrongLeader(req);
        }


        // ---------- state machine (only ever called from the RSM's reader thread) ----------

        json doOp(json req) override {
            string key = req["key"];
            json reply;

            if(req["op"] == "get"){
                if(!data.count(key)){
                    reply["err"] = ErrNoKey;
                    return reply;
                }
                reply["err"] = OK;
                reply["value"] = data[key].first;
                reply["version"] = data[key].second;
                return reply;
            }

            //Put - same rules as lab 2
            string value = req["value"];
            uint64_t version = req["version"];

            if(!data.count(key)){
                if(version != 0){
                    reply["err"] = ErrNoKey;
                    return reply;
                }
                data[key] = { value, 1 };
                reply["err"] = OK;
                return reply;
            }

            if(data[key].second != version){
                reply["err"] = ErrVersion;
                return reply;
            }

            data[key] = { value, version + 1 };
            reply["err"] = OK;
            return reply;
        }


        string snapshot() override {
            json snap = json::object();
            for(auto &[key, vv] : data){
                snap[key] = { vv.first, vv.second };
            }
            return snap.dump();
        }


        void restore(string snap_data) override {
            data.clear();
            json snap = json::parse(snap_data);
            for(auto &[key, vv] : snap.items()){
                data[key] = { vv[0].get<string>(), vv[1].get<uint64_t>() };
            }
        }


    private:

        //data has to be declared before rsm - members are destroyed in reverse order, so this way the
        //rsm (and its reader thread that calls doOp) is shut down before data goes away
        map<string, pair<string, uint64_t>> data;    //key -> (value, version)
        unique_ptr<RSM> rsm;


        json submitOrWrongLeader(json req){
            auto [err, result] = rsm->submit(req);
            if(err != OK){
                json reply;
                reply["err"] = ErrWrongLeader;
                return reply;
            }
            return result;
        }

};
