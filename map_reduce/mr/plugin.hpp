#pragma once

#include <string>
#include <iostream>
#include <cstdlib>
#include <dlfcn.h>
#include "common.hpp"

using namespace std;


//Loads an app's Map + Reduce out of a shared library (like the Go plugins the MIT lab uses), so the
//same worker binary can run wc, shoes, crash, ... without recompiling. Apps export them as extern "C"
//so the names don't get mangled and dlsym can find them.
void loadPlugin(string path, MapFunc &mapf, ReduceFunc &reducef){
    void* handle = dlopen(path.c_str(), RTLD_NOW);
    if(!handle){
        cerr << "cannot load plugin " << path << ": " << dlerror() << endl;
        exit(1);
    }

    mapf = (MapFunc) dlsym(handle, "Map");
    reducef = (ReduceFunc) dlsym(handle, "Reduce");
    if(!mapf || !reducef){
        cerr << "plugin " << path << " is missing Map or Reduce" << endl;
        exit(1);
    }
}
