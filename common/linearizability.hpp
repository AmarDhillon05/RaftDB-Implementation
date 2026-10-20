#pragma once

#include <vector>
#include <string>
#include <map>
#include <set>
#include <mutex>
#include <chrono>
#include <cstdint>
#include <algorithm>
#include "kv.hpp"

using namespace std;


//Linearizability checker for the versioned kv (a mini version of porcupine, which the MIT tests use).
//
//Clients record every Get/Put with when it was called + when it returned. The history is
//linearizable if we can pick one moment inside each op's [call, return] window where it "happened",
//such that running the ops one at a time in that order gives exactly the results the clients saw.
//
//Search is the Wing & Gong algorithm with the caching trick from Lowe's paper (same as porcupine):
//walk the history in time order, try to linearize any op whose call has happened, backtrack if we hit
//an op's return before it was linearized. Keys are independent so each key gets checked on its own.


struct KVOp {
    bool is_put;
    string key;
    string value;           //put: value written, get: value read
    uint64_t version;       //put: version sent, get: version read
    string err;             //what came back (OK, ErrNoKey, ErrVersion, ErrMaybe)
    int64_t call;           //ns timestamps
    int64_t ret;
};


//Thread safe log of ops that test clients append to
class History {

    public:

        static int64_t now(){
            return chrono::duration_cast<chrono::nanoseconds>(chrono::steady_clock::now().time_since_epoch()).count();
        }


        void add(KVOp op){
            lock_guard<mutex> lock(mu);
            ops.push_back(op);
        }


        vector<KVOp> all(){
            lock_guard<mutex> lock(mu);
            return ops;
        }


    private:

        mutex mu;
        vector<KVOp> ops;

};


//Wraps a clerk so every get/put it does gets recorded
class RecordingClerk : public IKVClerk {

    public:

        RecordingClerk(IKVClerk* ck, History* history){
            this->ck = ck;
            this->history = history;
        }


        GetResult get(string key) override {
            int64_t call = History::now();
            GetResult r = ck->get(key);
            history->add({ false, key, r.value, r.version, r.err, call, History::now() });
            return r;
        }


        string put(string key, string value, uint64_t version) override {
            int64_t call = History::now();
            string err = ck->put(key, value, version);
            history->add({ true, key, value, version, err, call, History::now() });
            return err;
        }


    private:

        IKVClerk* ck;
        History* history;

};


//State of one key in the model - version 0 means the key doesn't exist yet
struct KeyState {
    string value;
    uint64_t version = 0;

    bool operator<(const KeyState &o) const {
        return version != o.version ? version < o.version : value < o.value;
    }
};


//The model - given the key's state before an op, what states could it be in after, if the op is
//allowed to return what it returned? Empty = this op can't happen here.
//(ErrMaybe is the one with two answers: the put might have applied or might not have)
vector<KeyState> stepModel(const KeyState &s, const KVOp &op){
    if(!op.is_put){
        if(op.err == ErrNoKey){
            return s.version == 0 ? vector<KeyState>{ s } : vector<KeyState>{};
        }
        bool matches = s.version != 0 && op.value == s.value && op.version == s.version;
        return matches ? vector<KeyState>{ s } : vector<KeyState>{};
    }

    bool version_matches = s.version == op.version;
    KeyState applied = { op.value, op.version + 1 };

    if(op.err == OK){
        return version_matches ? vector<KeyState>{ applied } : vector<KeyState>{};
    }
    if(op.err == ErrVersion){
        return !version_matches && s.version != 0 ? vector<KeyState>{ s } : vector<KeyState>{};
    }
    if(op.err == ErrNoKey){
        return s.version == 0 && op.version != 0 ? vector<KeyState>{ s } : vector<KeyState>{};
    }
    if(op.err == ErrMaybe){
        return version_matches ? vector<KeyState>{ applied, s } : vector<KeyState>{ s };
    }
    return {};
}


//Checks one key's ops. Each op becomes a call event + a return event in a doubly linked list sorted by
//time; linearizing an op "lifts" both its events out of the list, backtracking puts them back.
bool checkKey(const vector<KVOp> &ops){
    int n = ops.size();
    if(n == 0){
        return true;
    }

    struct Event {
        bool is_call;
        int op;
        int64_t time;
        int prev = -1;
        int next = -1;
        int match = -1;     //call <-> return of the same op
    };

    //Build sorted events (returns before calls on ties, which is the stricter choice)
    vector<Event> events;
    for(int i = 0; i < n; i ++){
        events.push_back({ true, i, ops[i].call });
        events.push_back({ false, i, ops[i].ret });
    }
    sort(events.begin(), events.end(), [](const Event &a, const Event &b){
        if(a.time != b.time){
            return a.time < b.time;
        }
        return !a.is_call && b.is_call;
    });

    vector<int> call_of(n), ret_of(n);
    for(int e = 0; e < events.size(); e ++){
        (events[e].is_call ? call_of : ret_of)[events[e].op] = e;
    }

    //Linked list with a sentinel head at index events.size()
    int head = events.size();
    events.push_back({ true, -1, 0 });
    for(int e = 0; e < head; e ++){
        events[e].prev = e == 0 ? head : e - 1;
        events[e].next = e + 1 < head ? e + 1 : -1;
        events[e].match = events[e].is_call ? ret_of[events[e].op] : call_of[events[e].op];
    }
    events[head].next = 0;

    auto unlink = [&](int e){
        events[events[e].prev].next = events[e].next;
        if(events[e].next != -1){
            events[events[e].next].prev = events[e].prev;
        }
    };
    auto relink = [&](int e){
        events[events[e].prev].next = e;
        if(events[e].next != -1){
            events[events[e].next].prev = e;
        }
    };
    auto lift = [&](int call){
        unlink(call);
        unlink(events[call].match);
    };
    auto unlift = [&](int call){
        relink(events[call].match);
        relink(call);
    };

    //Which ops are linearized so far, as a bitset - (bitset, state) pairs we've seen before are dead ends
    vector<uint64_t> linearized((n + 63) / 64, 0);
    set<pair<vector<uint64_t>, KeyState>> cache;

    struct Frame {
        int call;
        KeyState state_before;
        vector<KeyState> options;   //possible states after this op
        int option;                 //which one we're on
    };
    vector<Frame> stack;

    KeyState state;
    int entry = events[head].next;

    while(events[head].next != -1){
        if(entry == -1){
            return false;
        }

        if(events[entry].is_call){
            int op = events[entry].op;
            vector<KeyState> options = stepModel(state, ops[op]);
            bool moved = false;

            for(int o = 0; o < options.size(); o ++){
                linearized[op / 64] |= (1ull << (op % 64));
                if(cache.insert({ linearized, options[o] }).second){
                    stack.push_back({ entry, state, options, o });
                    state = options[o];
                    lift(entry);
                    entry = events[head].next;
                    moved = true;
                    break;
                }
                linearized[op / 64] &= ~(1ull << (op % 64));
            }

            if(!moved){
                entry = events[entry].next;
            }
        }
        else {
            //Hit a return whose op isn't linearized yet - undo the last choice and try something else
            bool resumed = false;
            while(!stack.empty() && !resumed){
                Frame frame = stack.back();
                stack.pop_back();
                int op = events[frame.call].op;
                unlift(frame.call);
                state = frame.state_before;
                linearized[op / 64] &= ~(1ull << (op % 64));

                //Other outcomes of the same op first (ErrMaybe), then move on to later ops
                for(int o = frame.option + 1; o < frame.options.size(); o ++){
                    linearized[op / 64] |= (1ull << (op % 64));
                    if(cache.insert({ linearized, frame.options[o] }).second){
                        stack.push_back({ frame.call, state, frame.options, o });
                        state = frame.options[o];
                        lift(frame.call);
                        entry = events[head].next;
                        resumed = true;
                        break;
                    }
                    linearized[op / 64] &= ~(1ull << (op % 64));
                }

                if(!resumed){
                    entry = events[frame.call].next;
                    resumed = true;
                }
            }

            if(!resumed){
                return false;
            }
        }
    }
    return true;
}


//Checks a whole history, returns the first key that isn't linearizable (or "" if it all is)
string checkLinearizable(const vector<KVOp> &ops){
    map<string, vector<KVOp>> by_key;
    for(const KVOp &op : ops){
        by_key[op.key].push_back(op);
    }

    for(auto &[key, key_ops] : by_key){
        if(!checkKey(key_ops)){
            return key;
        }
    }
    return "";
}
