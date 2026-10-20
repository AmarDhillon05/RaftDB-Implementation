#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <chrono>
#include <csignal>
#include "../mr/coordinator.hpp"

using namespace std;


//Usage: mrcoordinator inputfiles...
//Starts the coordinator, waits for the job to finish, then exits
int main(int argc, char** argv){
    if(argc < 2){
        cerr << "Usage: mrcoordinator inputfiles..." << endl;
        return 1;
    }
    signal(SIGPIPE, SIG_IGN);

    vector<string> files(argv + 1, argv + argc);
    Coordinator coordinator(files, 10);

    while(!coordinator.done()){
        this_thread::sleep_for(chrono::seconds(1));
    }

    //Give workers a second to come back and get told to exit before we take the socket away
    this_thread::sleep_for(chrono::seconds(1));
    coordinator.shutdown();
    return 0;
}
