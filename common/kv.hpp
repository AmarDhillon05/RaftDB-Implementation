#pragma once

#include <string>
#include <cstdint>

using namespace std;


//Shared key/value types - the same Get/Put-with-version interface is used by the single server
//kv (lab 2), the raft kv (lab 4) and the sharded kv (lab 5), so things like the lock work on all of them


//Errors that come back from Get/Put
const string OK = "OK";
const string ErrNoKey = "ErrNoKey";
const string ErrVersion = "ErrVersion";
const string ErrMaybe = "ErrMaybe";             //a retried put got ErrVersion - can't tell if the first try went through
const string ErrWrongLeader = "ErrWrongLeader";
const string ErrWrongGroup = "ErrWrongGroup";


struct GetResult {
    string value;
    uint64_t version;
    string err;
};


//Anything that acts like a kv client
class IKVClerk {
    public:
        virtual GetResult get(string key) = 0;
        virtual string put(string key, string value, uint64_t version) = 0;
        virtual ~IKVClerk(){}
};
