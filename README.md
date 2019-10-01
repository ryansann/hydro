# Hydro Persistent Key Value Store

Hydro is a simple, persistent key value store with abstracted storage and indexing layers.

This was done as an experiment in databases for a Golang meetup.

To start the hydro tcp server run: `make`

You can connect to the server via `telnet localhost <port>`. Once connected you can run commands like:

- `set hello world` // sets key "hello" to "world"
- `get hello` // gets value for key "hello"
- `del hello` // deletes key hello
- `quit` // close connection

If you have restore enabled, the database will read the commit log to reload the key value store on startup.

The available environment variables to change runtime behavior are:

```bash
export HYDRO_INDEX_RESTORE=true
export HYDRO_INDEX_SYNC_INTERVAL=5s
export HYDRO_SERVER_PORT=:8888
export HYDRO_SERVER_READ_TIMEOUT=60s
export HYDRO_SERVER_SHUTDOWN_TIMEOUT=5s
```

## Future Work

- Implement Log Compaction
- Implement different indexing strategies, e.g. SSTable
- Implement different storage layers, e.g. paginated file storage
- Support clustering and replication across nodes
- Multiple indexes
