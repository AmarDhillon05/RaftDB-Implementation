# Lab 2 - Key/Value Server

A single machine kv server where every key has a **version**, plus a distributed lock built on top of it.

```
make test-kvsrv
```

## The interface

```
Get(key)                 -> value, version          (ErrNoKey if missing)
Put(key, value, version) -> OK                      if version == key's current version (then version++)
                            OK                      if key is missing and version == 0 (creates it at version 1)
                            ErrVersion / ErrNoKey   otherwise
```

The version check doubles as duplicate detection. If a Put went through but its reply got lost, the
client retries, but the version already moved on, so the retry gets `ErrVersion` and nothing applies twice.
No per-client table needed on the server.

## Why `ErrMaybe` exists

The client retries until it hears back. On a retry, `ErrVersion` is ambiguous:

- our first attempt applied and only its reply got lost, **or**
- someone else's Put got in first

We can't tell which, so the clerk returns `ErrMaybe`. (On the *first* attempt, `ErrVersion` really means it didn't apply.)

## The lock (`lock.hpp`)

The lock is one key. `""` means free, `<random id>` means held by that client.

- **acquire:** read the key. If it's free, Put my id at the version I read. Only one racing client can match that version. On `ErrMaybe`/`ErrVersion`, go back to the top and re-read. If the value is my id, my "maybe" put actually landed.
- **release:** read the key. If it's still my id, Put `""` at that version. If it's not mine anymore, an earlier `ErrMaybe` release already went through.

It only depends on `IKVClerk` (`common/kv.hpp`), so the same lock works on the raft kv (lab 4) and the sharded kv (lab 5).

## The fake network (`common/labrpc.hpp`)

Labs 2-5 all run in one process on a simulated network, so the tests can break things on purpose:

- `setReliable(false)` - random delays, 10% of requests and 10% of replies get dropped
- `enable(end, false)` - cut a link (partitions)
- `deleteServer` - "crash" a machine (in flight replies from it get lost)
- `setLongDelays` / `setLongReordering` - slow failures, out of order replies

`call()` returning false means "no reply", and like a real network you can't tell if the handler ran.

## Tests

- put/get + every error case
- concurrent read-then-put from 5 clients, reliable and unreliable. The final version has to equal `1 + #OK` (reliable), or land between `1 + #OK` and `1 + #OK + #ErrMaybe` (unreliable)
- single client on a lossy network, where every put must end up applied
- lock with 1 and 10 clients, reliable + unreliable. Inside the critical section a shared `holder` must be empty when you get in and still be you when you leave
