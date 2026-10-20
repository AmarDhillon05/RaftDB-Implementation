#pragma once

#include <vector>
#include <string>
#include <mutex>
#include <chrono>
#include "../../common/rpc.hpp"
#include "common.hpp"

using namespace std;


//The orchestrator - hands out map tasks, waits for all of them, then hands out reduce tasks
//Workers poll us for work (worker-initiated), so we never need to know any worker's address up front.
//We just keep the state of every task and answer "what should I do?" requests.


enum TaskState {
    IDLE,
    IN_PROGRESS,
    COMPLETED
};


struct Task {
    int id;
    TaskState state = IDLE;
    chrono::steady_clock::time_point started;
    string file;    //only used by map tasks
};


class Coordinator {

    public:

        Coordinator(vector<string> files, int n_reduce, chrono::milliseconds task_timeout = chrono::seconds(10)){
            this->n_reduce = n_reduce;
            this->task_timeout = task_timeout;

            //One map task per input file, nReduce reduce tasks
            for(int i = 0; i < files.size(); i ++){
                Task task;
                task.id = i;
                task.file = files[i];
                map_tasks.push_back(task);
            }
            for(int i = 0; i < n_reduce; i ++){
                Task task;
                task.id = i;
                reduce_tasks.push_back(task);
            }
            maps_left = map_tasks.size();
            reduces_left = reduce_tasks.size();

            server.registerHandler("RequestTask", [this](json args){ return requestTask(args); });
            server.registerHandler("ReportTask", [this](json args){ return reportTask(args); });
            server.start(coordinatorSock());
        }


        //Main loop of mrcoordinator calls this periodically to know when to exit
        bool done(){
            lock_guard<mutex> lock(mu);
            return reduces_left == 0;
        }


        void shutdown(){
            server.stop();
        }


    private:

        mutex mu;
        int n_reduce;
        chrono::milliseconds task_timeout;
        vector<Task> map_tasks;
        vector<Task> reduce_tasks;
        int maps_left;
        int reduces_left;
        RpcServer server;


        //Finds a task a worker can take - either never started, or started so long ago that we assume
        //the worker died. Instead of a separate ticker thread checking deadlines, we only check when
        //someone asks for work, since an expired task only matters once there's a free worker to take it.
        //Returns -1 if there's nothing to hand out
        int pickTask(vector<Task> &tasks){
            auto now = chrono::steady_clock::now();

            for(Task &task : tasks){
                bool expired = task.state == IN_PROGRESS && now - task.started > task_timeout;
                if(task.state == IDLE || expired){
                    task.state = IN_PROGRESS;
                    task.started = now;
                    return task.id;
                }
            }
            return -1;
        }


        //RPC: worker asks for work
        json requestTask(json args){
            lock_guard<mutex> lock(mu);
            json reply;
            reply["n_reduce"] = n_reduce;
            reply["n_map"] = map_tasks.size();

            //Map phase - reduces can't start until every map is done, since any mapper could
            //have written to any bucket
            if(maps_left > 0){
                int id = pickTask(map_tasks);
                if(id == -1){
                    reply["type"] = WAIT_TASK;
                }
                else {
                    reply["type"] = MAP_TASK;
                    reply["id"] = id;
                    reply["file"] = map_tasks[id].file;
                }
                return reply;
            }

            //Reduce phase
            if(reduces_left > 0){
                int id = pickTask(reduce_tasks);
                if(id == -1){
                    reply["type"] = WAIT_TASK;
                }
                else {
                    reply["type"] = REDUCE_TASK;
                    reply["id"] = id;
                }
                return reply;
            }

            reply["type"] = EXIT_TASK;
            return reply;
        }


        //RPC: worker says it finished a task
        //If the task was reassigned and both workers finish, the second report is just ignored - the
        //straggler's compute ran but we only count the first one. That's safe because workers write
        //to a temp file and rename it, so the output file is always one complete copy, never half of each.
        json reportTask(json args){
            lock_guard<mutex> lock(mu);
            int type = args["type"];
            int id = args["id"];

            vector<Task> &tasks = type == MAP_TASK ? map_tasks : reduce_tasks;
            int &left = type == MAP_TASK ? maps_left : reduces_left;

            if(id >= 0 && id < tasks.size() && tasks[id].state != COMPLETED){
                tasks[id].state = COMPLETED;
                left --;
            }

            return json::object();
        }

};
