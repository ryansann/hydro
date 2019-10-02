# Hydro

Hydro is a simple, persistent key / value store written in Go.

This was done as an experiment in databases for a Golang meetup.

To start the hydro tcp server run: `make`

You can connect to the server via `telnet localhost <port>`. Once connected you can run commands like:

- `set hello world` // sets key "hello" to "world"
- `get hello` // gets value for key "hello"
- `del hello` // deletes key "hello"
- `quit` // close connection

If you have restore enabled, the database will read the commit log to reload the key value store on startup.

The are hydro specific environment variables that will change runtime behavior. Those can be found [here](https://github.com/ryansann/hydro/blob/master/.envrc)

## Future Work

- Implement additional indexing / storage modes, e.g. SSTable, LSM
- Support clustering and replication across nodes
- Multiple indexes
