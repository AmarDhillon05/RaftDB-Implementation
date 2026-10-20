#pragma once

#include <string>
#include <memory>
#include "../common/labrpc.hpp"
#include "../common/kv.hpp"

using namespace std;


//Client side of the kv server. The network can drop requests or replies, so both calls just keep
//retrying until they hear back.


class Clerk : public IKVClerk {

    public:

        Clerk(shared_ptr<ClientEnd> server){
            this->server = server;
        }


        //Gets are always safe to retry, they don't change anything
        GetResult get(string key) override {
            json args;
            args["key"] = key;

            while(true){
                json reply;
                if(server->call("KVServer.Get", args, reply)){
                    GetResult result;
                    result.err = reply["err"];
                    result.value = result.err == OK ? reply["value"].get<string>() : "";
                    result.version = result.err == OK ? reply["version"].get<uint64_t>() : 0;
                    return result;
                }
                sleepMs(100);
            }
        }


        //Puts are trickier. If the first try gets ErrVersion, it definitely didn't apply. But if a
        //*retry* gets ErrVersion, maybe the first try did apply and only its reply got lost (which
        //bumped the version), or maybe someone else's put got in first - we can't tell, so ErrMaybe.
        string put(string key, string value, uint64_t version) override {
            json args;
            args["key"] = key;
            args["value"] = value;
            args["version"] = version;

            bool first_try = true;
            while(true){
                json reply;
                if(server->call("KVServer.Put", args, reply)){
                    string err = reply["err"];
                    if(err == ErrVersion && !first_try){
                        return ErrMaybe;
                    }
                    return err;
                }
                first_try = false;
                sleepMs(100);
            }
        }


    private:

        shared_ptr<ClientEnd> server;

};
