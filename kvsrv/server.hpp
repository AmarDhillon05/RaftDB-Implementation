#pragma once

#include <string>
#include <map>
#include <mutex>
#include <memory>
#include "../common/labrpc.hpp"
#include "../common/kv.hpp"

using namespace std;


//Single machine key/value server. Every key has a value + a version number.
//
//  Get(key)                 -> value, version (or ErrNoKey)
//  Put(key, value, version) -> only goes through if version matches the key's current version,
//                              and then bumps the version. version 0 on a missing key creates it.
//
//The version check is what makes retries safe - if a Put gets retried after it already went through,
//the version has moved on, so the retry gets ErrVersion instead of applying twice. That means the
//server needs no per-client duplicate table at all.


struct VersionedValue {
    string value;
    uint64_t version;
};


class KVServer {

    public:

        //Wraps the handlers up as a Server so it can be put on the network
        shared_ptr<Server> makeServer(){
            auto server = make_shared<Server>();
            server->addHandler("KVServer.Get", [this](json args){ return get(args); });
            server->addHandler("KVServer.Put", [this](json args){ return put(args); });
            return server;
        }


        json get(json args){
            lock_guard<mutex> lock(mu);
            string key = args["key"];
            json reply;

            if(!data.count(key)){
                reply["err"] = ErrNoKey;
                return reply;
            }

            reply["value"] = data[key].value;
            reply["version"] = data[key].version;
            reply["err"] = OK;
            return reply;
        }


        json put(json args){
            lock_guard<mutex> lock(mu);
            string key = args["key"];
            string value = args["value"];
            uint64_t version = args["version"];
            json reply;

            //New key - only version 0 is allowed to create it
            if(!data.count(key)){
                if(version != 0){
                    reply["err"] = ErrNoKey;
                    return reply;
                }
                data[key] = { value, 1 };
                reply["err"] = OK;
                return reply;
            }

            //Existing key - versions have to match
            if(data[key].version != version){
                reply["err"] = ErrVersion;
                return reply;
            }

            data[key] = { value, version + 1 };
            reply["err"] = OK;
            return reply;
        }


    private:

        mutex mu;
        map<string, VersionedValue> data;

};
