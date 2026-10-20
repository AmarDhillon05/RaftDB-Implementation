# Lab 1 - Map Reduce

The design follows what got worked out in `rambling.md`. This is the "how it ended up" summary for later.

## Running it

```
make mr          # builds coordinator, worker, sequential + the app plugins into build/
make test-mr     # wc, shoes, early exit, crash tests (crash takes a few minutes)
```

By hand (from the repo root):

```
./build/mrcoordinator map_reduce/test/data/dbnode_* &
./build/mrworker build/shoes.so &
./build/mrworker build/shoes.so &
cat mr-out-*
```

## Files

| file | what it is |
|---|---|
| `common/rpc.hpp` | tiny json RPC over a unix socket (one connection per call) |
| `mr/common.hpp` | `KeyValue`, the `MapFunc`/`ReduceFunc` types, task types, file naming, `ihash` |
| `mr/coordinator.hpp` | the orchestrator - task state + the two RPC handlers |
| `mr/worker.hpp` | worker loop, `doMap`, `doReduce` |
| `mr/plugin.hpp` | loads `Map`/`Reduce` out of an app `.so` with dlopen |
| `apps/` | `wc` (word count), `shoes` (avg price per brand, the original test mapper/reducer), `crash` (wc that randomly dies/stalls) |
| `main/` | `mrcoordinator`, `mrworker`, `mrsequential` (single process reference answer) |
| `test/` | data generators + `test-mr.sh` |

## The flow

1. Coordinator gets the input files, makes one map task per file and `nReduce` (10) reduce tasks.
2. Workers **poll** the coordinator (`RequestTask`). Worker-initiated, so the coordinator never needs a list of worker addresses and workers can come and go.
3. Map task `m`: read the file, call `Map(filename, contents)`, put each pair into bucket `ihash(key) % nReduce`, **sort each bucket**, write it to `mr-m-b`.
4. Once *every* map is done (any mapper could have written to any bucket) the coordinator starts handing out reduce tasks.
5. Reduce task `r`: open `mr-0-r ... mr-(M-1)-r`. They're each sorted, so do a **k-way merge** with a min-heap and call `Reduce(key, values)` once per key. Only one key's values are in memory at a time. Output goes to `mr-out-r`.
6. When all reduces are reported, `RequestTask` answers `EXIT_TASK` and everyone shuts down.

## Failures / stragglers

- Every task records when it was handed out. If a task has been `IN_PROGRESS` for more than 10s it's treated as free again. Instead of a ticker thread this is checked lazily in `pickTask` - an expired task only matters when a worker is free to take it.
- Stragglers aren't killed. The slow worker keeps going, and whichever `ReportTask` comes in first counts; later reports for a completed task are ignored.
- That's only safe because every file is written **temp file + `rename()`**. Rename is atomic, and map/reduce are deterministic, so whoever renames last still leaves one complete, correct file.

## Things I'd look at next

- Backup tasks (paper section 3.6) - hand out a second copy of the last few in-progress tasks proactively instead of waiting for the timeout.
- Tree-style reduction (mentioned in the rambling).
