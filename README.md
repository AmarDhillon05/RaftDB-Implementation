# RaftDB

The MIT 6.5840 (distributed systems) labs, done from scratch in C++ instead of Go: MapReduce, then a
key/value store that grows into a Raft-replicated, sharded database.

Every lab has its own README with how it works, and each lab is its own branch, built on the one before it:

| branch | lab | what |
|---|---|---|
| `lab1-mapreduce` | [map_reduce/](map_reduce/README.md) | coordinator + workers over a unix socket RPC, apps as plugins |
| `lab2-kvsrv` | [kvsrv/](kvsrv/README.md) | single server kv with versioned puts, plus a lock built on it |
| `lab3-raft` | [raft/](raft/README.md) | Raft (one commit per part: 3A election, 3B log, 3C persistence, 3D snapshots) |
| `lab4-kvraft` | [kvraft/](kvraft/README.md) | the kv replicated on Raft through a generic RSM layer |
| `lab5-shardkv` | [shardkv/](shardkv/README.md) | sharded across Raft groups, with a crash-safe shard controller |

## Building + testing

Needs g++ (C++17) and make. JSON is [nlohmann/json](https://github.com/nlohmann/json), a submodule in `lib/json`:

```
git submodule update --init
make test-mr        # lab 1
make test-kvsrv     # lab 2
make test-raft      # lab 3   (T=3A etc. for one part)
make test-kvraft    # lab 4
make test-shardkv   # lab 5
```

## Shared stuff (`common/`)

| file | what |
|---|---|
| `rpc.hpp` | real RPC over unix sockets (lab 1 only, since coordinator/workers are separate processes) |
| `labrpc.hpp` | fake in-process network for labs 2-5, which can drop, delay, reorder, partition and crash on command |
| `channel.hpp` | Go style channel (raft's apply channel) |
| `kv.hpp` | the Get/Put-with-version interface + error codes every kv lab shares |
| `linearizability.hpp` | a small porcupine: checks a recorded history of kv ops is linearizable |
| `testing.hpp` | tiny test runner |

## How the pieces stack

```
                     ShardClerk
                         |  key -> shard -> group
          +--------------+---------------+
          v              v               v
     shard group 1   shard group 2   ...      <- each one: ShardGrp state machine
          |                                          |
          +------------- RSM (lab 4) ----------------+   submit -> raft -> apply in order
                          |
                        Raft (lab 3)                     replicated log
                          |
                    labrpc network                       (fake, so tests can break it)
```

The controller (`shardkv/shardctrler.hpp`) keeps its configs in one more raft group running the lab 4 kv.

## Differences from the Go version

- Raft log commands are `json` instead of `interface{}`, and RPC args/replies are json too.
- Map reduce apps are `.so` plugins loaded with `dlopen`, which is the closest thing to Go plugins.
- Tests are ports of the MIT tests with the same names where they exist. Where the MIT tests use porcupine, these use `common/linearizability.hpp`.
