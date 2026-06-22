# CLI tests

These are the end-to-end tests for the `tg` command line interface. Each `.nu`
file is one self-contained test, run by the harness in `packages/cli/test.nu`.
Run them with `nu packages/cli/test.nu [pattern]`, where `pattern` is an
optional regex matched against each test's path. Pass `--accept` to accept new
or changed snapshots.

## Conventions

### 1. Every test begins with an intent comment

After the `use` line, write a one-to-three sentence comment stating the single
behavior the test asserts, as a plain declarative sentence. State the intent,
not the mechanics that the code already makes obvious. Avoid boilerplate
prefixes such as "Verifies that" or "This test".

```nushell
use ../../test.nu *

# <The single behavior that must hold>.
```

Comments are complete sentences that end in periods and do not use contractions.

### 2. One behavior per test

A test verifies a single behavior and ideally takes a single snapshot (or a
tight cluster of snapshots for that one behavior). Reuse the `spawn`,
`artifact { ... }`, `snapshot`, `success`, and `failure` helpers from
`test.nu`. When a file would test several independent behaviors, split it into
one file per behavior.

### 3. Assert with inline snapshots, not `str contains`

Assert on output with an exact inline `snapshot`, never with `str contains`. A
substring check only confirms a fragment appears somewhere; it silently passes
over everything around it, so a regression that mangles the rest of the message
goes unnoticed. A snapshot captures the whole normalized output, so it both
documents the exact behavior and fails when anything drifts:

```nushell
# Not this:
assert ($output.stderr | str contains 'the process is already finished') "…"

# This — run with --accept to fill the body:
snapshot ($output.stderr | redact $path) '
	-> the process is already finished
'
```

Normalize first with `redact` and `normalize_ids` (see the next convention) so
the snapshot is stable. A check for the *absence* of a string becomes a snapshot
of the whole output, which verifies the absence implicitly: if the output no
longer contains the string, the snapshot will not either.

When the output is genuinely non-deterministic in ordering or volume — for
example the fan-out lines a parallel `publish` prints — do not fall back to
`str contains`. Extract the lines that matter and sort them into a deterministic
subset, then snapshot that:

```nushell
let tagged = $output.stderr | lines | where {|l| $l =~ 'info tagged'} | sort
snapshot $tagged '…'
```

`str contains` is fine for non-assertion control flow, such as a `where` filter
that selects lines. It is the *assertion* that must be a snapshot.

### 4. Normalize nondeterministic output before snapshotting

Never hand-roll `str replace` normalization before a snapshot. Two helpers from
`test.nu` cover the two kinds of nondeterminism, and they compose.

The `redact` helper collapses the data that genuinely varies from run to run:
runtime IDs that the server assigns (`pcs_…` becomes `<process>`, `err_…`
becomes `<error>`, and likewise for sandboxes, users, groups, and
organizations), numeric process IDs in `id = …` lines, and the paths passed as
arguments (which become `<path>`):

```nushell
let stderr = $output.stderr | redact $path
```

The `normalize_ids` helper canonicalizes the content-addressed object IDs that
are deterministic but noisy and brittle — files, directories, commands, and so
on. It rewrites each distinct ID to a stable per-kind token such as `fil_0100`,
preserving identity (the same ID renders the same token) while staying robust to
the serialization changes that churn the underlying hashes. Use it for
snapshots that surface object IDs, such as tree views or error stacks:

```nushell
let stderr = $output.stderr | redact $path | normalize_ids
```

Reach for `redact` alone when the output has no object IDs worth showing,
`normalize_ids` alone for pure structure such as a tree, and the pipeline above
when a snapshot carries both.

### 5. Synchronize with `wait_until`, not `sleep`

Never wait for the system with a bare `sleep` or a hand-rolled polling loop.
Use the `wait_until` helper from `test.nu`, which polls a condition and errors
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
feature, a missing external tool — call the `skip_test` helper from `test.nu`
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
await tg.run`echo "$TANGRAM_TOKEN" && sleep 60`.env(tg.build(busybox)).sandbox();
```

Use the object form (`tg.run({ args, env, executable, host })`) only when the
template form cannot express the invocation, or when the object form itself is
the behavior under test.

### 9. Gate network access behind `skip_if_offline`

A test which performs real network I/O — downloading from an external URL,
tagging busybox with `spawn --busybox` — must call `skip_if_offline` at the
top. Running the suite with `--offline` skips these tests, so the rest of the
suite is hermetic:

```sh
nu packages/cli/test.nu --offline
```

The `spawn --busybox` helper calls `skip_if_offline` itself, so a test only
needs an explicit call when it reaches the network some other way.

### 10. Name principals `alice`, `bob`, `carol`, and reserve `eve` for the adversary

In tests that log in users and exercise authorization, name ordinary
cooperating principals `alice`, `bob`, `carol`, and so on. Reserve `eve` for the
malicious principal — the one attempting to read information or escalate
privileges they should not have. When a test demonstrates that an unauthorized
read or a privilege escalation is denied, the denied actor is `eve`, and the
intent comment frames the operation as one that must be rejected:

```nushell
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

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
  because the `spawn` helper starts a server.
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
