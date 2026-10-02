# CLI tests

These are the end-to-end tests for tangram. Each `.nu`
file is one self-contained test, run by the harness in `packages/cli/test.nu`.
Run them with `nu packages/cli/test.nu [pattern]`, where `pattern` is an
optional regex matched against each test's path. Pass `--accept` to accept new
or changed snapshots. Pass `--preserve-failing-temps` to keep the temporary
directories for failed tests, or `--preserve-temps` to keep them for every
test. Pass `--no-progress-details` to show only the aggregate progress bar
during highly concurrent runs.

## Cloud databases

Cloud database tests are supported on Linux and macOS. On Linux, install native
FoundationDB, PostgreSQL, ScyllaDB 2026.3 or later, and `nats-server`. On macOS,
install Docker, PostgreSQL, and `nats-server`. Then start the shared databases in
a separate terminal:

```sh
nu packages/cli/test.nu --databases
```

The command runs every database as a native process on Linux. On macOS, it runs
PostgreSQL and NATS as native processes and FoundationDB and ScyllaDB in Docker.
It waits for them to become ready and removes their temporary state after
Ctrl-C. Existing services, processes, or containers must not be using ports
5432, 4500, 9042, or 4222. Database output is written to the log directory shown
at startup and tailed automatically if startup fails or a database exits
unexpectedly.
The runner builds and uses `tangram_scylla_client` from
`packages/scylla/client` for ScyllaDB administration.

On Linux and macOS, the normal test command runs cloud tests against the shared
databases:

```sh
nu packages/cli/test.nu [pattern]
```

A Tangram instance contains servers directly or is divided into regions, each
of which contains one or more servers. Regionless instances need no topology
arguments. For a regional instance, use `instance --regions` to declare its
regions and `instance --primary-region` to select its primary region, then place
each server with `server spawn --instance ... --region ...`. Instance
configuration is inherited by every server, while `server spawn --config`
applies only to that server. A server in a regional instance must select one of
the declared regions. Topology belongs to the instance and the `--region`
argument, not per-server configuration.

Cloud-backed servers in the same instance share PostgreSQL and NATS. Servers in
the same region also share their FoundationDB index instance and ScyllaDB store;
different regions receive isolated storage.

For tests that do not need an explicit topology, `server spawn` creates a local
instance automatically, while `server spawn --cloud` creates a cloud-backed
instance automatically. Use `server stop`, `server start`, and `server restart`
to manage the returned server record without repeating its arguments. The
record includes the resolved `config` and its `config_path`.

The runner removes these resources when the test finishes. Use `--clean` while
the databases are running to remove resources left behind by interrupted test
runs. Pass `--no-cloud` to use local backends instead.

## Conventions

### 1. Every test begins with an intent comment

After the `use` line, write a one-to-three sentence comment stating the single
behavior the test asserts, as a plain declarative sentence. State the intent,
not the mechanics that the code already makes obvious. Avoid boilerplate
prefixes such as "Verifies that" or "This test".

```nushell
use ../lib/test.nu *

# <The single behavior that must hold>.
```

Comments are complete sentences that end in periods and do not use contractions.

### 2. One behavior per test

A test verifies a single behavior and ideally takes a single snapshot (or a
tight cluster of snapshots for that one behavior). Reuse the `server spawn`,
`artifact { ... }`, `snapshot`, `success`, and `failure` helpers from
`lib/test.nu`. Tests import this helper module, not the runner in `../test.nu`, so
each test avoids parsing runner-only code. When a file would test several
independent behaviors, split it into one file per behavior.

To assert on what the server itself logged, spawn it with
`--config { tracing: { stderr_format: 'json' } }` and snapshot `server_errors`,
which stops the server so its output is complete and returns the distinct
errors it logged. An empty snapshot asserts that it logged none.

### 3. Match the assertion to the behavior

Use an inline `snapshot` when the exact, deterministic CLI output is the
behavior being specified. This documents the full message and catches changes
to its wording or formatting. Use snapshot normalization and redaction flags
(see the next convention) to keep it stable:

```nushell
snapshot --normalize --redact $path $output.stderr '
	-> the process is already finished
'
```

Use `assert` for a focused property: for example, that a protocol event occurs,
a streamed value reaches the client, or sensitive data is absent. A substring
check is appropriate when presence or absence of that value is the property
under test. Do not use it as a stand-in for checking an entire diagnostic when
the full deterministic message matters.

When output varies in ordering or volume — for example the fan-out lines a
parallel `publish` prints — select the lines that matter and sort them into a
deterministic subset, then snapshot that:

```nushell
let tagged = $output.stderr | lines | where {|l| $l =~ 'info tagged'} | sort
snapshot $tagged '…'
```

`str contains` can also be used in non-assertion control flow, such as a
`where` filter that selects lines.

### 4. Normalize nondeterministic output before snapshotting

Never hand-roll `str replace` normalization before a snapshot. The snapshot
flags cover the different kinds of nondeterminism, and they compose.

Use `--normalize` for data that genuinely varies from run to run: runtime IDs
that the server assigns (`pcs_…` becomes a stable `pcs_0000…`, and likewise
for errors, sandboxes, users, groups, and organizations), numeric process IDs
in `id = …` lines, and tokens. Distinct IDs remain distinct. This deliberately
preserves content-addressed object IDs so tests can snapshot them exactly:

```nushell
snapshot --normalize $output.stderr '…'
```

Use `--normalize-ids` only when exact IDs are not part of the behavior under
test. It applies the same normalization to runtime data and additionally
rewrites content-addressed IDs to stable values such as `fil_0100…`, preserving
identity while staying robust to changes that churn the underlying hashes:

```nushell
snapshot --normalize-ids $output.stderr '…'
```

Use `--redact` only for specific literal values such as temporary paths. It
accepts a string or a list of strings and replaces each with `<redacted>`:

```nushell
snapshot --normalize --redact $path $output.stderr '…'
snapshot --normalize --redact [$path $server.directory] $output.stderr '…'
```

### 5. Synchronize with `wait_until`, not `sleep`

Never wait for the system with a bare `sleep` or a hand-rolled polling loop.
Use the `wait_until` helper from `lib/test.nu`, which polls a condition and errors
with a clear message after a timeout:

```nushell
wait_until { tg watch list | from json | is-empty } "the watch should be removed after its ttl expires"
```

This runs as soon as the condition holds and tolerates slow machines. If there
is genuinely no observable condition to poll, keep the `sleep` and add a
comment explaining why.

### 6. A test is either a behavior test or a load test, never both

- **Behavior tests** assert an edge case or a correctness property via
  `snapshot`, `success`, `failure`, or `assert`. They are deterministic.
- **Load tests** exercise the system under a specific kind of load —
  concurrency, volume, restarts, races, or deadlock regressions. They assert
  only liveness or non-failure (the work completes, nothing hangs, nothing
  crashes), never a fine-grained correctness snapshot.

When a test both stresses the system and asserts correctness, split it: keep the
correctness assertion in a small behavior test, and move the load to a separate
load test that asserts only liveness.

A load test that guards against a specific historical bug ends its intent
comment with a provenance line referencing the commit that fixed the bug (or
the commit that introduced the test, when the fix cannot be attributed):

```nushell
# Regression test for 4819305a (#734).
```

### 7. Skip tests whose prerequisites are missing

When a test cannot run in the current environment — a platform-specific
feature, a missing external tool — call the `skip_test` helper from `lib/test.nu`
with the reason instead of returning early or failing:

```nushell
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
```

The runner reports skipped tests separately from passed and failed tests, so a
silent early return never masquerades as a pass.

### 8. Prefer the template form of `tg.run` and `tg.build`

In test artifacts, invoke commands with the tagged template form rather than
the object form:

```typescript
await tg.run`echo "$TANGRAM_TOKEN" && sleep 60`
	.env(tg.build(busybox))
	.sandbox();
```

Use the object form (`tg.run({ args, env, executable, host })`) only when the
template form cannot express the invocation, or when the object form itself is
the behavior under test.

### 9. Gate network access behind `skip_if_offline`

A test which performs real network I/O — downloading from an external URL,
tagging busybox with `server spawn --busybox` — must call `skip_if_offline` at the
top. Running the suite with `--offline` skips these tests, so the rest of the
suite is hermetic:

```sh
nu packages/cli/test.nu --offline
```

The `server spawn --busybox` helper calls `skip_if_offline` itself, so a test only
needs an explicit call when it reaches the network some other way.

### 10. Use a local HTTP server for download tests

Import `lib/http.nu` and call `spawn_http_server` with responses keyed by URL
path. The helper listens on an OS-assigned port, waits for startup,
and uses the test harness to clean up the server, including on failure or timeout.
These tests can run with `--offline`.

```nushell
use ../lib/http.nu *

let http = spawn_http_server {
	'/file.txt': { body: "hello, world!\n" },
	'/archive.tar.gz': { file: $archive_path },
	'/redirect': { status: 302, headers: { location: '/file.txt' } },
	'/missing': { status: 404 },
}
let local = server spawn
let output = tg download $'($http.url)/file.txt' --checksum sha256:any | complete
success $output
```

Responses accept `body` (text), `file` (a local file path for binary contents),
`headers`, and `status` (200 by default). Unconfigured paths return 404. Redact
`$http.url` in diagnostic snapshots to keep the random port out of expectations.
Start the HTTP fixture before `server spawn`: on Linux it configures test-only
pasta/passt wrappers so sandboxed downloads can reach host loopback. Use
`$http.host_url` for commands that run directly on the host.
Pass URLs as build arguments to keep test modules independent of the port.

### 11. Name principals `alice`, `bob`, `carol`, and reserve `eve` for the adversary

In tests that log in users and exercise authorization, name ordinary
cooperating principals `alice`, `bob`, `carol`, and so on. Reserve `eve` for the
malicious principal — the one attempting to read information or escalate
privileges they should not have. When a test demonstrates that an unauthorized
read or a privilege escalation is denied, the denied actor is `eve`, and the
intent comment frames the operation as one that must be rejected:

```nushell
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# A user without read permission cannot get another user's record.
let output = tg --token $eve.token user get $alice.user.id | complete
failure $output "a user without read permission should not be able to get another user"
```

## Non-scriptable surface

Most `tg` subcommands are scriptable and have end-to-end tests here. A handful
are intentionally excluded because they cannot be driven by the harness, mutate
the host, or are exercised indirectly. They are documented here rather than
tested:

- `view` — the interactive terminal UI. It has no scriptable output.
- `repl` and `js` — interactive evaluators that read from a live terminal.
- `lsp` — a language server over stdio. Its transport is exercised indirectly by
  the tests under `lsp/`.
- `serve` (and `server run`) — the foreground daemon. Every test exercises it,
  because the `server spawn` helper starts a server.
- `shell activate`, `shell deactivate`, and `shell integration` — these mutate
  the host shell environment.
- `self update` — it replaces the running binary.
- `builtin` and the hidden `sandbox serve`/`container`/`seatbelt`/`vm`
  subcommands — platform plumbing exercised indirectly by every sandboxed run.

When a new top-level command is added, it needs either a test here or an entry
in this list.

## Stress testing

To flush out rare races, run a test repeatedly with the worker pool kept full
until it fails:

```sh
nu packages/cli/test.nu --stress 'run/sandbox_dequeue_finish_deadlock'
```

Pass `--stress-count N` to bound the number of rounds instead of running until
failure. Stress mode reports the round on which a test failed, and may not be
combined with `--accept` or `--review`.
