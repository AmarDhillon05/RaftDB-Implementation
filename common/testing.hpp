#pragma once

#include <string>
#include <vector>
#include <functional>
#include <iostream>
#include <chrono>
#include <cstdlib>
#include <unistd.h>

using namespace std;


//Bare minimum test runner so we don't need gtest - a test is a name + a function, and check()
//fails the whole run on the spot (works from any thread, since it just prints and exits)


string current_test;


void check(bool cond, string msg){
    if(!cond){
        cerr << "--- FAIL: " << current_test << ": " << msg << endl;
        _exit(1);
    }
}


//Runs every test whose name contains argv[1] (or all of them with no args), prints timings
int runTests(vector<pair<string, function<void()>>> tests, int argc, char** argv){
    string filter = argc > 1 ? argv[1] : "";
    int ran = 0;

    for(auto &[name, test] : tests){
        if(name.find(filter) == string::npos){
            continue;
        }

        current_test = name;
        cout << "=== RUN   " << name << endl;
        auto start = chrono::steady_clock::now();

        test();

        double secs = chrono::duration<double>(chrono::steady_clock::now() - start).count();
        cout << "--- PASS: " << name << " (" << secs << "s)" << endl;
        ran ++;
    }

    cout << "PASS (" << ran << " tests)" << endl;
    return 0;
}
