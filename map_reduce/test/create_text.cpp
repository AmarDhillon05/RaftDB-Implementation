#include <fstream>
#include <vector>
#include <string>
#include <random>
#include <iostream>

using namespace std;

//Generates fake text files for word count tests - words are picked with a skewed distribution
//so some show up a ton and some barely at all (like real text)
//Usage: create_text outdir

int main(int argc, char** argv){
    if(argc != 2){
        cerr << "Usage: create_text outdir" << endl;
        return 1;
    }
    string outdir = argv[1];

    vector<string> words = {
        "raft", "leader", "follower", "candidate", "term", "vote", "log", "entry", "commit",
        "apply", "snapshot", "shard", "key", "value", "put", "get", "append", "map", "reduce",
        "worker", "coordinator", "partition", "bucket", "network", "crash", "restart", "the",
        "a", "of", "and", "to", "in", "is", "that", "it", "for", "on", "with", "as", "was"
    };

    //Fixed seed so every run makes the same files
    mt19937 rng(42);
    geometric_distribution<int> pick(0.12);
    uniform_int_distribution<int> line_len(3, 15);

    for(int f = 0; f < 8; f ++){
        ofstream ofs(outdir + "/text_" + to_string(f) + ".txt");

        for(int line = 0; line < 2000; line ++){
            int n = line_len(rng);
            for(int w = 0; w < n; w ++){
                int idx = pick(rng) % words.size();
                ofs << words[idx] << (w + 1 == n ? "\n" : " ");
            }
        }
    }

    return 0;
}
