# Setup and development guide

## Requirements

- Go 1.21 or newer (developed on 1.22)
- Python 3 (only to generate the sample data file)
- `make`

No databases, no Docker, no external services. M1–M3 is a single process.

## First run

```bash
git clone https://github.com/aadithyaa9/mini-spark.git
cd mini-spark

go build ./...        # should be silent
make test             # should be green
make example          # word count with the stage plan printed
```

If `make example` prints a lineage tree with a `+-` marker and a two-stage plan,
everything works.

## Make targets

| Target | What it does |
|---|---|
| `make test` | `go test -count=1 ./...` |
| `make race` | same, under the race detector |
| `make vet` | `go vet ./...` |
| `make fmt` | `go fmt ./...` |
| `make example` | generate sample data, run word count with `-v` |
| `make bench` | time word count at parallelism 1/2/4/8 |
| `make clean` | remove generated data and the test cache |

## Reading the code

Roughly in dependency order; each file makes more sense once the one above it
is understood.

| Order | File | Why it matters |
|---|---|---|
| 1 | `pkg/funcs/registry.go` | The closure workaround. Nothing else makes sense until this does. |
| 2 | `pkg/core/iterator.go` | Laziness. Every transformation is an iterator wrapper. |
| 3 | `pkg/core/dependency.go` | Narrow vs wide. This distinction *is* the stage-splitting algorithm. |
| 4 | `pkg/core/rdd.go` | The RDD interface and why it holds a recipe, not data. |
| 5 | `pkg/rdds/narrow.go` | How small each concrete RDD actually is. |
| 6 | `pkg/sched/dag.go` | `getOrCreateParentStages` — where the graph gets cut. |
| 7 | `pkg/shuffle/manager.go` | The data/index file layout. |
| 8 | `pkg/api/pair_ops.go` | `CombineByKey`, and the partitioner-reuse check. |

## Running one test

```bash
go test ./test/ -run TestShuffleIsSkippedWhenAlreadyPartitioned -v
go test ./test/ -run TestTextFileSplitBoundaries -v
```

That second one is worth reading before changing anything in `textfile.go`. It
catches the classic split-boundary off-by-one, where a boundary landing exactly
on a newline causes a whole line to be silently dropped.

## Debugging a job

Set `Verbose: true` on the config to log the stage plan:

```go
sc, _ := api.NewContext(api.Config{Verbose: true})
```

Print a lineage tree at any point:

```go
fmt.Println(sc.DebugString(rdd))
```

The tree shows partition counts, partitioners, and cache levels. `+-` marks a
shuffle boundary. If you expect an optimization to have removed a shuffle and
the `+-` is still there, it did not fire.

### Common errors

| Message | Cause |
|---|---|
| `"foo" not registered` | The function was never registered, or registered in a package that was not imported. Registration happens in `init()`, which only runs if something imports the package. |
| `expected core.KV record, got string` | A pair operation was called on a non-pair RDD. Map to `core.KV` first. |
| `panic in user code (stage N, partition M)` | A type assertion failed inside a user function. The task fails, not the process; the stack trace is in the error. |
| `serde: encode ...: gob: type not registered` | A custom struct is being used as a key or value. Call `serde.RegisterType(MyType{})`. |

## Adding a transformation

Narrow transformations are mechanical. Copy `MappedRDD` in `pkg/rdds/narrow.go`:

1. Embed `core.BaseRDD`, hold the parent and the function name.
2. Construct with `core.NewOneToOneDependency(parent)`.
3. `Partitions()` returns the parent's.
4. `Compute` looks up the function and wraps the parent iterator.
5. Set `r.PartFn = parent.Partitioner()` **only if** records cannot move
   between partitions. Getting this wrong causes silently wrong results, not a
   crash, because downstream operations will trust it and skip a shuffle they
   needed.
6. Add a one-line wrapper in `pkg/api/transformations.go`.

Wide transformations go through `Context.CombineByKey`. If you can express it as
create/merge/merge, you get map-side combining and partitioner reuse for free.

## Building milestone 4 (distribution)

The local scheduler was written so that distribution touches as little as
possible. In order:

1. **`pkg/rpc`** — define the proto, generate clients. Two services:
   `MasterService` (register, heartbeat, map-output registry) and
   `WorkerService` (launch task, fetch shuffle block).

2. **Make the task serializable.** Today `sched.Task` holds a `*Stage` with a
   live `core.RDD` pointer. It needs an `RDDDesc` — a recursive, gob-encodable
   description of the lineage that a worker rebuilds via a kind registry. This
   is the largest single piece of work in M4.

3. **Split the map-output registry out of `shuffle.Manager`.** It already has
   the right shape (`NumMaps`, `UnregisterMapOutput`); it just needs to live on
   the master and be reachable over RPC. `core.ShuffleService` does not change.

4. **Replace `DAGScheduler.runTask`.** It becomes "serialize the task, send it
   to a worker, await the status update." The stage splitting, topological
   ordering, and retry logic above it stay exactly as they are — that is the
   point of the current structure.

5. **Fetch shuffle blocks over HTTP.** `Manager.readBlock` already seeks to an
   offset and reads a length; the remote version does the same through a range
   request.

6. **Then, and only then, fault tolerance.** Worker heartbeat timeout →
   unregister its map outputs → the DAG scheduler sees the parent stage is no
   longer available → resubmit. `Stage.NewAttempt` exists for this.

Do not start on the web UI until step 6 passes a test that kills a worker
mid-stage.

## Committing

Small commits with messages that say *why*, not *what*. `git diff` already shows
what changed.

Bad: `update scheduler`, `fix`, `final changes`
Good: `shuffle: skip empty blocks using MapStatus sizes`,
`textfile: don't drop a line when a split boundary lands on a newline`

Commit when a test goes green, not when a file is finished.
