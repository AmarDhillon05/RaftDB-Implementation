# Lab 3 - Raft

Raft keeps a log identical on a set of servers, as long as a majority can talk to each other. The code
follows Figure 2 of the [paper](https://raft.github.io/raft.pdf) nearly line for line, so keep that open
while reading `raft.hpp`.

```
make test-raft          # everything (~6 min)
make test-raft T=3A     # just one part (3A, 3B, 3C, 3D) or one test name
```

Each part is its own commit on this branch, so `git log -p raft/raft.hpp` walks through it in order.

## API (what the kv server in lab 4 uses)

| call | does |
|---|---|
| `Raft::make(peers, me, persister, apply_ch)` | create + start the background threads |
| `start(cmd)` | leader only: append to the log, return `(index, term, is_leader)` right away (no promise it commits) |
| `getState()` | `(term, is_leader)` |
| `snapshot(index, data)` | the service saved everything through `index`, so raft can drop that much of its log |
| apply channel | one `ApplyMsg` per committed command, in order, on every server, or a snapshot to load |

## Threads

- **ticker** - every 10ms: a leader sends heartbeats every 100ms, anyone else starts an election once its randomized 300-600ms timer runs out.
- **applier** - waits for `commit_index > last_applied` and pushes commands onto the apply channel. It's the *only* thread that pushes, so nothing goes out of order, and it pushes without holding `mu` so the service can call back into raft.
- **one detached thread per RPC** - so a slow or dead peer never blocks anything. Each one captures a `shared_ptr` to the Raft so it stays alive until the call returns.

Every RPC reply handler re-checks `state == LEADER && current_term == the term I sent with`, since by the time a reply shows up the world may have moved on.

## 3A - elections

- Timeout -> become candidate, `term++`, vote for self, persist, ask everyone in parallel.
- Vote only if we haven't voted this term **and** the candidate's log is at least as up to date as ours (last term bigger, or same last term and at least as long). That's what guarantees a new leader already has every committed entry.
- Only reset the election timer when a vote is *granted* (or on a valid AppendEntries), otherwise a server with a stale log can keep everyone from timing out.
- Seeing a higher term anywhere (request or reply) -> `stepDown`: adopt the term, clear the vote, become follower.

## 3B - log replication

- `start` appends and broadcasts right away instead of waiting for the next heartbeat.
- AppendEntries carries `prev_log_index/term`. The follower rejects if it doesn't have a matching entry there.
- **Only truncate on an actual conflict.** A delayed, older AppendEntries must not chop off entries a newer one already added.
- **Fast backup:** on a reject, the follower sends `x_term` (the conflicting term), `x_index` (where that term starts in its log), and `x_len` (its log length), so the leader skips a whole term per round trip instead of one entry.
- **Commit rule:** the leader only advances `commit_index` to an index N if a majority has it **and** `log[N].term == current_term`. Figure 8 shows how committing an old-term entry by counting replicas goes wrong. Old entries get committed indirectly once a current-term entry after them commits.
- Follower commit is `max(commit_index, min(leader_commit, last new entry index))`. It never goes backwards, and never past what this RPC proved matches.

## 3C - persistence

`current_term`, `voted_for`, `log` (plus snapshot index/term in 3D) are saved via `persist()` every time they change, **before** replying or sending anything that depends on them. E.g. if we vote, crash, and forget the vote, we could vote twice in one term and get two leaders. On restart `readPersist` loads them back.

## 3D - snapshots

Once the service snapshots up through index `i`, everything before `i` is dropped. `log[0]` becomes a dummy entry standing in for the snapshot (`snapshot_index`, `snapshot_term`), and `log[k]` is really index `snapshot_index + k`. All the code goes through `termAt`/`commandAt`/`lastLogIndex`, which do the translation.

- Leader: if `next_index[peer] <= snapshot_index`, the entries that peer needs are gone, so it sends **InstallSnapshot** instead.
- Follower InstallSnapshot: ignore it if we already committed past it. If our log contains the snapshot's last entry (same term), keep the suffix after it, otherwise drop the whole log. Then persist state + snapshot together and bump `commit_index`.
- The applier notices `last_applied < snapshot_index` and sends the snapshot up the channel before any more commands, so ordering stays right.
- AppendEntries whose `prev_log_index` is inside our snapshot: skip the part we already have (it's committed so it has to match).
- Raft state and snapshot are always saved in **one** `persister->save` call, so a crash can't leave one without the other.

## Testing (`test/`)

`config.hpp` is a port of the MIT `config.go` harness. It runs n Rafts on the fake network, and every applied command gets checked against every other server's command at that index. `raft_test.cpp` has every test from `test_test.go` with the same names.

The one deliberate change is that commands are `json` instead of `interface{}`, so they can be ints, strings, or whole kv ops in lab 4.
