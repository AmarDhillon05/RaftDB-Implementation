#pragma once

#include <deque>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <chrono>

using namespace std;


//Go-style channel (unbounded) - one side push()es, the other side pop()s and blocks until something
//shows up. Raft uses one of these to hand committed commands to the service above it (applyCh).
template <typename T>
class Channel {

    public:

        void push(T item){
            {
                lock_guard<mutex> lock(mu);
                if(closed){
                    return;
                }
                items.push_back(move(item));
            }
            cond.notify_one();
        }


        //Blocks until there's an item, returns nullopt once the channel is closed + empty
        optional<T> pop(){
            unique_lock<mutex> lock(mu);
            cond.wait(lock, [this](){ return !items.empty() || closed; });
            return takeFront();
        }


        //Same as pop() but gives up after a timeout
        optional<T> popFor(chrono::milliseconds timeout){
            unique_lock<mutex> lock(mu);
            cond.wait_for(lock, timeout, [this](){ return !items.empty() || closed; });
            return takeFront();
        }


        void close(){
            {
                lock_guard<mutex> lock(mu);
                closed = true;
            }
            cond.notify_all();
        }


    private:

        mutex mu;
        condition_variable cond;
        deque<T> items;
        bool closed = false;


        optional<T> takeFront(){
            if(items.empty()){
                return nullopt;
            }
            T item = move(items.front());
            items.pop_front();
            return item;
        }

};
