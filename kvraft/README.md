# Lab 4 - Fault Tolerant Key/Value Service

The lab 2 versioned kv, replicated with lab 3 raft so it survives crashes and partitions.

```
make test-kvraft          # everything (~3 min)
make test-kvraft T=4A     # 4A = rsm layer, 4B = kv, 4C = snapshots
```

## Layers

```
Clerk  --RPC-->  KVServer  --submit()-->  RSM  --start()-->  Raft
                    ^                      |
                    |  doOp(req) <---------+  (apply channel, every server, in log order)
```

- **`rsm.hpp`** (4A) is the generic part. A service implements `StateMachine` (`doOp`, `snapshot`, `restore`) and calls `submit(req)`. Lab 5's shard groups reuse it unchanged.
- **`server.hpp`** is the kv `StateMachine`, with the exact same Get/Put rules as lab 2.
- **`client.hpp`** is the lab 2 clerk plus finding the leader.

## How `submit` works

1. Lock, `rf->start(op)` (op = `{me, unique id, req}`), and register as waiting on the returned index - **while still holding the lock**.
2. The reader thread pulls each committed entry off the apply channel, runs `doOp`, and if someone's waiting on that index, hands them the result.
3. The waiter checks the entry at its index is actually *its* op (`me` + `id` match). A new leader could have put something else there. If not, `ErrWrongLeader`.
4. While waiting, every 50ms check `getState()`. If the term changed or we're not leader anymore, our entry may never commit, so give up with `ErrWrongLeader`.

> Bug I hit: step 1 originally registered the waiter *after* `start()` returned. If the entry committed and got applied in that tiny gap, nobody ever woke the submitter and `TestBasic4B` hung about 90% of the time. Holding the lock across both fixes it. It's deadlock-free because the lock order is always rsm -> raft (raft never calls up into the rsm).

## Duplicate detection = versions

There's no client id / sequence number table. A retried Put that already applied gets `ErrVersion`, because the version moved on. The clerk counts *any* failed attempt (no reply, or `ErrWrongLeader`) as "might have applied", since a leader can append a put, lose leadership, and the next leader still commits it. So `ErrVersion` after a failed attempt becomes `ErrMaybe`.

## Gets go through the log too

Otherwise a leader that's been partitioned off (and doesn't know it's been replaced) would serve stale data. I checked this. Making Gets local on whoever thinks it's leader makes the partition tests fail the linearizability check right away.

## Snapshots (4C)

After applying each entry, if `persister.raftStateSize() >= maxraftstate`, the rsm calls `rf->snapshot(index, sm->snapshot())`. On restart the rsm restores the persisted snapshot before raft replays the rest of the log. When raft delivers a snapshot from the leader, the rsm calls `sm->restore`.

## Tests

- **4A** - the rsm on its own with a counter state machine: basic, concurrent submits, leader failure, a submit stuck in a minority returns `ErrWrongLeader`, replay after restart, shutdown doesn't hang, snapshots.
- **4B/4C** - `genericTest` (like MIT's GenericTest): 3 rounds of clients doing get-then-put, optionally with an unreliable network, random partitions every ~second, and crash+restart of all servers between rounds. Every op is recorded and **the whole history goes through the linearizability checker** (`common/linearizability.hpp`, a small porcupine). Plus explicit partition, snapshot-install, snapshot-size and speed tests.
