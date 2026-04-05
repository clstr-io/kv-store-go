# clstr.io Key-Value Store

Reference implementation for the [clstr.io distributed key-value store challenge](https://clstr.io/).

## Architecture

Key-Value Store is a persistent HTTP-based key-value store built in layers. The HTTP API handles client requests and validation, while the storage layer provides in-memory access with durability guarantees through write-ahead logging.

### Storage Layer

At the core is an in-memory key-value map protected by a read-write mutex, allowing concurrent reads while serializing writes. This provides the baseline functionality: fast lookups with thread-safe access.

The disk store wraps this in-memory map and adds persistence through a write-ahead log (WAL). Every mutation is first appended to the log and synced to disk before being applied to memory. This ensures that even if the process crashes, all acknowledged writes can be recovered by replaying the log.

To prevent the WAL from growing unbounded, the system periodically creates snapshots of the entire in-memory state and truncates the log. On startup, recovery loads the most recent snapshot and replays any operations logged after it.

#### Batching for Throughput

Synchronous disk writes are expensive, and requiring a disk sync per operation creates severe lock contention under concurrent load. To address both issues, the store batches multiple operations together and performs a single `fsync` for the entire batch. Operations are flushed either when the batch reaches a size threshold or after a time interval, trading individual operation latency for significantly higher aggregate throughput and reduced lock contention.

### HTTP API

The API layer exposes the key-value store over HTTP with PUT, GET, DELETE, and CLEAR operations. Keys are validated to contain only safe characters, and values are size-limited to prevent resource exhaustion attacks from unbounded memory consumption. Request statistics are tracked to provide visibility into performance characteristics.

## Performance

### After Completing Stage 3

Typical performance on modern hardware:

| Operation          | Latency      | Notes                                         |
|--------------------|--------------|-----------------------------------------------|
| Reads (GET)        | ~15µs        | In-memory with shared lock                    |
| Writes (light)     | ~20-40ms     | Waiting for batch to fill                     |
| Writes (heavy)     | ~140ms avg   | Batching + queueing under concurrent load     |

All write operations (PUT, DELETE, CLEAR) go through the WAL and exhibit similar performance characteristics. Batching trades individual operation latency for significantly higher aggregate throughput under concurrent load.

## Running

```console
$ go build -o key-value-go ./cmd/key-value-go
$ DATA_DIR=./data ./key-value-go
```

The server listens on port 8080. Configuration is via environment variables:

- `DATA_DIR` — directory to persist data (default: `/app/data`)
- `PEERS` — comma-separated list of peer node addresses (e.g. `host1:8080,host2:8080`)

The server persists all data to `DATA_DIR` and recovers it on restart. Graceful shutdown (Ctrl+C or SIGTERM) ensures proper cleanup and data integrity.

## Testing

Test with [clstr](https://clstr.io/guides/cli/):

```console
$ clstr test http-api
$ clstr test persistence
$ clstr test <stage>
```
