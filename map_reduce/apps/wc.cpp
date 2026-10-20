#include <vector>
#include <string>
#include <cctype>
#include "../mr/common.hpp"

using namespace std;

//Word count - the classic map reduce example. Map emits (word, "1") for every word, reduce counts them.


extern "C" vector<KeyValue> Map(string filename, string contents){
    vector<KeyValue> kvs;
    string word;

    //A word is any run of letters
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
    return to_string(values.size());
}
