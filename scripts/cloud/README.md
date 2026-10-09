# Local cloud runner

Run these commands from the repository root. This starts a cloud API backed by
PostgreSQL, FoundationDB, NATS, and Scylla, plus a separate runner with local
backends. The client uses its own directory and sends `--remote` builds to that API.

1. Start the infrastructure in a terminal and leave it running:

   ```sh
   nu packages/cli/test.nu --databases
   ```

   Install the missing programs reported by the command. On Linux this uses
   native database servers; on macOS it also requires Docker. Tangram needs a
   FoundationDB client library compatible with the running server.

2. Build the binaries and initialize the stores once, then start the API:

   ```sh
   bun run cloud:init
   bun run cloud:server
   ```

3. Start the runner in another terminal:

   ```sh
   bun run cloud:runner
   ```

4. Start the client and build:

   ```sh
   bun run cloud:tg server start
   bun run cloud:tg build --remote /absolute/path/to/project
   ```

On Linux, the runner and client require delegated CPU and memory cgroups. If your
shell does not provide them, run steps 3 and 4 from shells launched with:

```sh
systemd-run --user --scope -p Delegate=yes -p DelegateSubgroup=supervisor "$SHELL"
```

This requires a systemd user session with systemd 254 or newer. Clear any
`TANGRAM_*` environment overrides before using these scripts.

State lives in a temporary directory linked at `.tangram/cloud` to keep Unix socket
paths short. The API listens on `127.0.0.1:8476` without user authentication.
Stop the client with `bun run cloud:tg server stop` and stop
the API and runner with Ctrl-C. Restart with the same commands; runner registration
and signing keys are retained. To reset, stop all three servers and run
`bun run cloud:deinit` while the databases are still running (`fdbcli` required).
This removes only the `tangram_cloud` database, keyspace, FoundationDB prefix, and
local state. Reset before restarting the temporary database supervisor or using
`test.nu --clean`, then initialize again.

The existing `bun run cloud:up` Kubernetes infrastructure also works. Supply its
FoundationDB cluster file with `bun run cloud:init --fdb-cluster /path/to/fdb.cluster`
(the Kubernetes cluster string is `docker:docker@127.0.0.1:4500`).
