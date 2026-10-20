#include <vector>
#include <string>
#include <random>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <unistd.h>
#include "../mr/common.hpp"

using namespace std;

//Same as word count, except it randomly crashes or stalls in the middle of tasks.
//Used by the crash test to make sure the coordinator reassigns work from dead/slow workers.


//Helper to maybe crash or hang - 1/3 chance to die, 1/3 chance to stall for a while
void maybeCrash(){
    static mt19937 rng(random_device{}() ^ getpid());
    int roll = rng() % 1000;

    if(roll < 330){
        _exit(1);
    }
    else if(roll < 660){
        this_thread::sleep_for(chrono::milliseconds(rng() % 10000));
    }
}


extern "C" vector<KeyValue> Map(string filename, string contents){
    maybeCrash();

    vector<KeyValue> kvs;
    string word;
    for(char c : contents){
        if(isalpha((unsigned char) c)){
            word.push_back(c);
        }
        else if(!word.empty()){
            kvs.push_back({ word, "1" });
            word.clear();
        }
    }
    if(!word.empty()){
        kvs.push_back({ word, "1" });
    }
    return kvs;
}


extern "C" string Reduce(string key, vector<string> values){
    maybeCrash();
    return to_string(values.size());
}
