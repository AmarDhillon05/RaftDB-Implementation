#pragma once

#include <string>
#include <vector>
#include <unistd.h>

using namespace std;


//Shared stuff between the coordinator and workers


//One emitted pair from map() - everything is strings so any app can encode whatever it wants
struct KeyValue {
    string key;
    string value;
};


//The plug-and-play part of map reduce - apps (wc.cpp, shoes.cpp, ...) only have to write these two
//map takes a file (name + contents) and turns it into k, v data
//reduce takes every value for one key and aggregates them into one output value
typedef vector<KeyValue> (*MapFunc)(string filename, string contents);
typedef string (*ReduceFunc)(string key, vector<string> values);


//What a worker gets told to do when it asks the coordinator for work
enum TaskType {
    MAP_TASK = 0,
    REDUCE_TASK = 1,
    WAIT_TASK = 2,     //nothing free right now but the job isn't done, ask again in a bit
    EXIT_TASK = 3      //job is done, worker can shut down
};


//Hash to decide which reduce bucket a key goes to (FNV-1a) - every mapper has to agree on this so
//"dog" always ends up in the same bucket no matter which mapper saw it
int ihash(const string &key){
    unsigned int hash = 2166136261u;
    for(char c : key){
        hash ^= (unsigned char) c;
        hash *= 16777619u;
    }
    return hash & 0x7fffffff;
}


//Intermediate files are named mr-<mapper>-<bucket>, so reducer r reads mr-0-r, mr-1-r, ...
//This is the "predetermined file names" convention - nobody has to tell a reducer where its data is
string intermediateName(int map_id, int bucket){
    return "mr-" + to_string(map_id) + "-" + to_string(bucket);
}

string outputName(int reduce_id){
    return "mr-out-" + to_string(reduce_id);
}


//Socket the coordinator listens on (per-user so two people on one machine don't collide)
string coordinatorSock(){
    return "/tmp/ratdb-mr-" + to_string(getuid());
}
