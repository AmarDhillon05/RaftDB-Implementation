#pragma once

#include <string>
#include <map>
#include <functional>
#include <thread>
#include <atomic>
#include <cstring>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::json;


//Tiny RPC over a unix socket - every call is one connection, one json line in and one json line out
//Coordinator + workers all live on one machine (same as the MIT lab) so a unix socket is all we need
//Request:  {"method": "...", "args": {...}}
//Response: {"ok": true, "reply": {...}} or {"ok": false, "error": "..."}


//Helper to read from a socket until a newline, returns false if the other side hung up first
bool readLine(int fd, string &out){
    out.clear();
    char buf[4096];

    while(true){
        ssize_t n = ::read(fd, buf, sizeof(buf));
        if(n <= 0){
            return false;
        }

        out.append(buf, n);
        size_t newline = out.find('\n');
        if(newline != string::npos){
            out.resize(newline);
            return true;
        }
    }
}


//Helper to write a full string to a socket, since write() is allowed to only do part of it
bool writeAll(int fd, const string &data){
    size_t sent = 0;
    while(sent < data.size()){
        ssize_t n = ::write(fd, data.data() + sent, data.size() - sent);
        if(n <= 0){
            return false;
        }
        sent += n;
    }
    return true;
}


//Helper to fill out a sockaddr_un for a path
sockaddr_un socketAddress(const string &path){
    sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    return addr;
}


class RpcServer {

    public:

        //Handlers take the args json and return the reply json
        void registerHandler(string method, function<json(json)> handler){
            handlers[method] = handler;
        }


        //Binds to the socket path and starts accepting on a background thread
        bool start(string socket_path){
            path = socket_path;
            ::unlink(path.c_str());

            listen_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
            if(listen_fd < 0){
                return false;
            }

            sockaddr_un addr = socketAddress(path);
            if(::bind(listen_fd, (sockaddr*) &addr, sizeof(addr)) < 0){
                return false;
            }
            if(::listen(listen_fd, 128) < 0){
                return false;
            }

            running = true;
            accept_thread = thread([this](){ acceptLoop(); });
            return true;
        }


        void stop(){
            if(!running){
                return;
            }
            running = false;

            //Shutting down the listening socket kicks accept() out so the thread can finish
            ::shutdown(listen_fd, SHUT_RDWR);
            ::close(listen_fd);
            accept_thread.join();
            ::unlink(path.c_str());
        }


        ~RpcServer(){
            stop();
        }


    private:

        string path;
        int listen_fd = -1;
        atomic<bool> running{false};
        thread accept_thread;
        map<string, function<json(json)>> handlers;


        void acceptLoop(){
            while(running){
                int conn_fd = ::accept(listen_fd, nullptr, nullptr);
                if(conn_fd < 0){
                    continue;
                }

                //One thread per call, so a slow handler doesn't block everyone else
                thread([this, conn_fd](){ handleConnection(conn_fd); }).detach();
            }
        }


        void handleConnection(int conn_fd){
            string line;
            json response;

            if(readLine(conn_fd, line)){
                try {
                    json request = json::parse(line);
                    string method = request["method"];

                    if(handlers.count(method)){
                        response["ok"] = true;
                        response["reply"] = handlers[method](request["args"]);
                    }
                    else {
                        response["ok"] = false;
                        response["error"] = "no such method " + method;
                    }
                }
                catch(exception &e){
                    response["ok"] = false;
                    response["error"] = e.what();
                }

                writeAll(conn_fd, response.dump() + "\n");
            }

            ::close(conn_fd);
        }

};


//Calls a method on the server at socket_path, returns false if the server couldn't be reached
//(which for map reduce usually just means the coordinator finished and exited)
bool rpcCall(string socket_path, string method, json args, json &reply){
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if(fd < 0){
        return false;
    }

    sockaddr_un addr = socketAddress(socket_path);
    if(::connect(fd, (sockaddr*) &addr, sizeof(addr)) < 0){
        ::close(fd);
        return false;
    }

    json request;
    request["method"] = method;
    request["args"] = args;

    string line;
    bool ok = writeAll(fd, request.dump() + "\n") && readLine(fd, line);
    ::close(fd);

    if(!ok){
        return false;
    }

    json response = json::parse(line);
    if(!response["ok"]){
        return false;
    }

    reply = response["reply"];
    return true;
}
