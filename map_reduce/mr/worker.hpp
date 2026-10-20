#pragma once

#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <queue>
#include <thread>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include "../../common/rpc.hpp"
#include "common.hpp"

using namespace std;


//A worker is just a loop: ask the coordinator for a task, do it, report it, repeat.
//It holds no state between tasks, so if it crashes the coordinator just hands the task to someone else.


//Helper to read a whole file into a string
string readFile(string path){
    ifstream ifs(path);
    stringstream ss;
    ss << ifs.rdbuf();
    return ss.str();
}


//Helper to write a file atomically - write everything to a temp file, then rename it into place.
//rename() is atomic, so readers either see no file or the full file, never a half-written one
//(this is what makes it safe to let a straggler keep running after its task got reassigned)
void writeFileAtomic(string path, string contents){
    string tmp_template = path + ".tmp-XXXXXX";
    vector<char> tmp_path(tmp_template.begin(), tmp_template.end());
    tmp_path.push_back('\0');

    int fd = mkstemp(tmp_path.data());
    writeAll(fd, contents);
    ::close(fd);

    ::rename(tmp_path.data(), path.c_str());
}


//Map task: run map() on the file, then split the output into nReduce buckets by key hash.
//Each bucket gets sorted before it's written so reducers can do a merge-style read across buckets.
void doMap(MapFunc mapf, int map_id, string filename, int n_reduce){
    string contents = readFile(filename);
    vector<KeyValue> kvs = mapf(filename, contents);

    //Put each pair in the bucket its key hashes to
    vector<vector<KeyValue>> buckets(n_reduce);
    for(KeyValue &kv : kvs){
        buckets[ihash(kv.key) % n_reduce].push_back(kv);
    }

    //Sort + write every bucket (even empty ones, so reducers can always assume the file exists)
    for(int b = 0; b < n_reduce; b ++){
        sort(buckets[b].begin(), buckets[b].end(), [](const KeyValue &a, const KeyValue &c){
            return a.key < c.key;
        });

        //One json object per line, so keys/values with spaces or newlines don't break anything
        string out;
        for(KeyValue &kv : buckets[b]){
            json line;
            line["k"] = kv.key;
            line["v"] = kv.value;
            out += line.dump() + "\n";
        }

        writeFileAtomic(intermediateName(map_id, b), out);
    }
}


//Reads the next pair from an intermediate file, returns false at the end
bool nextKeyValue(ifstream &ifs, KeyValue &kv){
    string line;
    if(!getline(ifs, line) || line.empty()){
        return false;
    }

    json parsed = json::parse(line);
    kv.key = parsed["k"];
    kv.value = parsed["v"];
    return true;
}


//Reduce task: merge every mapper's (already sorted) file for this bucket, group values by key,
//and call reduce() once per key. Since the inputs are sorted we only ever hold one key's values
//in memory at a time, instead of loading every file fully.
void doReduce(ReduceFunc reducef, int reduce_id, int n_map){

    //Open mr-0-r, mr-1-r, ...
    vector<ifstream> files;
    for(int m = 0; m < n_map; m ++){
        files.emplace_back(intermediateName(m, reduce_id));
    }

    //Min-heap of (pair, which file it came from) - the top is always the smallest key left anywhere
    typedef pair<KeyValue, int> HeapItem;
    auto cmp = [](const HeapItem &a, const HeapItem &b){ return a.first.key > b.first.key; };
    priority_queue<HeapItem, vector<HeapItem>, decltype(cmp)> heap(cmp);

    for(int m = 0; m < n_map; m ++){
        KeyValue kv;
        if(nextKeyValue(files[m], kv)){
            heap.push({ kv, m });
        }
    }

    string out;
    while(!heap.empty()){

        //Pull every value for the smallest key, refilling the heap from whichever file we took from
        string key = heap.top().first.key;
        vector<string> values;

        while(!heap.empty() && heap.top().first.key == key){
            HeapItem item = heap.top();
            heap.pop();
            values.push_back(item.first.value);

            KeyValue kv;
            if(nextKeyValue(files[item.second], kv)){
                heap.push({ kv, item.second });
            }
        }

        out += key + " " + reducef(key, values) + "\n";
    }

    writeFileAtomic(outputName(reduce_id), out);
}


//Main worker loop - runs until the coordinator says to exit (or can't be reached, which means it's gone)
void runWorker(MapFunc mapf, ReduceFunc reducef){
    while(true){
        json reply;
        if(!rpcCall(coordinatorSock(), "RequestTask", json::object(), reply)){
            return;
        }

        int type = reply["type"];

        if(type == EXIT_TASK){
            return;
        }
        if(type == WAIT_TASK){
            this_thread::sleep_for(chrono::milliseconds(200));
            continue;
        }

        int id = reply["id"];
        if(type == MAP_TASK){
            doMap(mapf, id, reply["file"], reply["n_reduce"]);
        }
        else {
            doReduce(reducef, id, reply["n_map"]);
        }

        //Tell the coordinator we're done (ignore failure, if it's gone the next request will exit us)
        json report;
        report["type"] = type;
        report["id"] = id;
        json ignored;
        rpcCall(coordinatorSock(), "ReportTask", report, ignored);
    }
}
