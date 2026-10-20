#pragma once

#include <string>
#include "../common/labrpc.hpp"
#include "../common/kv.hpp"

using namespace std;


//Distributed lock built on top of any kv clerk. The lock is just a key:
//  value ""        -> free
//  value <my id>   -> held by me
//Grabbing it is a versioned Put from "" to my id, so if two clients race, only one Put can match the
//version and the other gets ErrVersion. Works over an unreliable network since ErrMaybe is resolved
//by reading the key back and seeing whose id is in it.


//Helper to make a random id for each lock holder
string randomId(){
    string chars = "abcdefghijklmnopqrstuvwxyz0123456789";
    string id;
    for(int i = 0; i < 12; i ++){
        id += chars[randInt(0, chars.size())];
    }
    return id;
}


class Lock {

    public:

        Lock(IKVClerk* ck, string lock_key){
            this->ck = ck;
            this->lock_key = lock_key;
            this->id = randomId();
        }


        //Blocks until we hold the lock
        void acquire(){
            while(true){
                GetResult current = ck->get(lock_key);

                //Already ours (an earlier ErrMaybe put actually went through)
                if(current.err == OK && current.value == id){
                    return;
                }

                //Free (missing key counts as free, version 0 creates it)
                if(current.err == ErrNoKey || current.value == ""){
                    string err = ck->put(lock_key, id, current.version);
                    if(err == OK){
                        return;
                    }
                    //ErrMaybe/ErrVersion - loop around and read who has it now
                }
                else {
                    sleepMs(10);   //someone else has it, wait a bit
                }
            }
        }


        void release(){
            while(true){
                GetResult current = ck->get(lock_key);
                if(current.err != OK || current.value != id){
                    return;   //not ours (anymore) - an earlier ErrMaybe release went through
                }

                string err = ck->put(lock_key, "", current.version);
                if(err == OK){
                    return;
                }
            }
        }


    private:

        IKVClerk* ck;
        string lock_key;
        string id;

};
