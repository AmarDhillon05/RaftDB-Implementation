#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include "../mr/worker.hpp"
#include "../mr/plugin.hpp"

using namespace std;


//Usage: mrsequential app.so inputfiles...
//Runs the whole job in one process with no parallelism, writing mr-out-0.
//This is the "known good" answer the tests compare the distributed output against.
int main(int argc, char** argv){
    if(argc < 3){
        cerr << "Usage: mrsequential app.so inputfiles..." << endl;
        return 1;
    }

    MapFunc mapf;
    ReduceFunc reducef;
    loadPlugin(argv[1], mapf, reducef);

    //Map every file into one big list
    vector<KeyValue> intermediate;
    for(int i = 2; i < argc; i ++){
        vector<KeyValue> kvs = mapf(argv[i], readFile(argv[i]));
        intermediate.insert(intermediate.end(), kvs.begin(), kvs.end());
    }

    //Sort so equal keys are next to each other (stable so values keep map order, like the real thing)
    stable_sort(intermediate.begin(), intermediate.end(), [](const KeyValue &a, const KeyValue &b){
        return a.key < b.key;
    });

    //Reduce each run of equal keys
    string out;
    int i = 0;
    while(i < intermediate.size()){
        int j = i;
        vector<string> values;
        while(j < intermediate.size() && intermediate[j].key == intermediate[i].key){
            values.push_back(intermediate[j].value);
            j ++;
        }

        out += intermediate[i].key + " " + reducef(intermediate[i].key, values) + "\n";
        i = j;
    }

    writeFileAtomic("mr-out-0", out);
    return 0;
}
