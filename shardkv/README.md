# Lab 5 - Sharded Key/Value Service

Keys are split into 12 shards, and each shard is served by one replica group (a lab 4 style raft group).
More groups means more throughput. A controller moves shards between groups as groups join and leave.

```
make test-shardkv          # everything (~1.5 min)
make test-shardkv T=5A     # 5A = moving shards, 5B = controller crashes, 5C = concurrent controllers
```

## Pieces

| file | what it is |
|---|---|
| `shardcfg.hpp` | `ShardConfig`: `num`, shard -> gid, gid -> server names. `join`/`leave` make the next config with shards rebalanced (minimal moves, deterministic) |
| `shardgrp/server.hpp` | a replica group, i.e. the lab 4 kv `StateMachine` per shard + Freeze/Install/Delete, all through the same `RSM` |
| `shardgrp/client.hpp` | `GroupClerk`, which finds a group's leader. Gives up after a few laps so a dead group doesn't hang callers |
| `shardctrler.hpp` | the controller, storing configs in a lab 4 kv group and moving shards |
| `client.hpp` | `ShardClerk`, which routes key -> shard -> group and refetches the config on `ErrWrongGroup` |

## Moving a shard

For config `N`, every shard whose owner changes goes:

```
FreezeShard(shard, N)        at the old group -> stops serving it, returns its data
InstallShard(shard, data, N) at the new group -> starts serving it
DeleteShard(shard, N)        at the old group -> frees the memory
```

Then the controller publishes config `N` and clients start routing by it. Until then, a client that
hits the frozen shard gets `ErrWrongGroup`, re-asks the controller, and retries.

**Every shard remembers the highest config num that touched it, and ignores anything older.** That's
what makes all the failure cases safe:

- Freeze with an old num gets `ErrVersion` (someone already moved this shard further), and the controller stops.
- Install only applies if `num > shard.num`, so a duplicate or late install can't overwrite writes that clients made after the first install. (I mutation-tested this: make install always overwrite and `TestPartitionCtrler5C` loses keys.)
- Delete only applies if `num >= shard.num`, so a late delete can't wipe a shard the group got back in a newer config.

Group 1 starts out owning every shard (num 1), matching the first config.

## The controller and its two keys

Configs live in a fault tolerant kv (the lab 4 raft kv) under:

- `"config"` - current config (clients read this)
- `"next"` - the change in progress

`changeConfigTo(next)`:

1. **Claim:** a versioned Put of `next` into `"next"`. Two controllers racing, only one Put matches the version. The loser (or anyone who finds a change already in flight) reads `"next"` back and *helps finish that change* instead.
2. **Move** every shard that changes owner (freeze/install/delete, tagged with `next.num`).
3. **Publish:** a versioned Put of `next` into `"config"`. It's versioned so it happens exactly once even if several controllers get here.

**5B - controller crashes:** `"next"` survives in the kv, so a new controller's `initController()` sees `next.num > config.num` and redoes steps 2-3. Redoing moves is harmless (see above).

**5C - several controllers:** the claim in step 1 serializes them, and everything after it is idempotent. A controller that got partitioned mid-change and comes back later just replays stale moves, which the groups reject or ignore by num, and its final publish fails its version check.

## Clients and ErrMaybe

Same rule as before, but tracked across groups. If any attempt might have applied (no reply,
`ErrWrongLeader`), a later `ErrVersion`, even from a *different* group after the shard moved, becomes
`ErrMaybe`. That's because an applied put's bumped version travels with the shard data.

## Tests

- **5A:** init/query, a down group only blocks *its* keys, data survives join, the old group actually frees moved data (snapshot shrinks), join+leave then shut down the old group, many join/leaves, shutting down retired groups, plus concurrent clients during reconfigs (reliable + unreliable). The concurrent tests record every op and run them through the **linearizability checker**.
- **5B:** a controller killed at a random point mid-change, then a fresh controller recovers it. With and without concurrent clients.
- **5C:** 3 controllers making the same change at once (it happens exactly once), and a controller partitioned mid-change that comes back after two newer configs.
