#include <iostream>
#include <csignal>
#include "../mr/worker.hpp"
#include "../mr/plugin.hpp"

using namespace std;


//Usage: mrworker app.so
//Loads the app's Map/Reduce and keeps asking the coordinator for work until the job's done
int main(int argc, char** argv){
    if(argc != 2){
        cerr << "Usage: mrworker app.so" << endl;
        return 1;
    }
    signal(SIGPIPE, SIG_IGN);

    MapFunc mapf;
    ReduceFunc reducef;
    loadPlugin(argv[1], mapf, reducef);

    runWorker(mapf, reducef);
    return 0;
}
