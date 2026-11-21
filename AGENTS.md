# Agents

Tangram is a build system and package manager that makes builds and tests fast and reliable.

## Git

Write commit messages in lowercase.

Use simple lowercase, dash-separated branch names, such as `standardize-ttl-naming`. Do not use slashes or prefixes such as `nitsky/`.

Work in the current checkout. Do not create additional Git worktrees.

Force pushing branches other than `main` is allowed. Never force push `main`.

When a change spans several pull requests that build on each other, open them as a GitHub stack, not as separate pull requests with chained bases. Use `gh stack init` and `gh stack submit` to create the stack, or `gh stack link <bottom> ... <top>` to link existing branches or pull requests from bottom to top. Merge a stack with `gh stack merge <stack> --yes --squash`.

## Repository scripts

Use Tangram as the interface for repository scripts: `tg run .#<export>` from the root, or `tg run ./packages/<package>#<export>` for a package. Pass script arguments after `--`, such as `tg run ./packages/tests#test -- --offline build`. These exports run tools directly on the host without sandboxing. Keep OS-specific operations in the existing Nushell, Python, and Node scripts as appropriate.

## Checking

Run `tg run .#check` to check changes for errors. Never use `cargo clippy --package` or `cargo check --package` as they do not check all crates. Always use `tg run .#check` which runs clippy on the entire workspace. Clear clippy warnings when you complete tasks.

Tangram supports Linux and macOS. When writing or changing code, account for conditional compilation so the code remains correct and warning-free on both platforms. When fixing warnings, inspect platform-specific code and preserve behavior on both platforms. Conditional compilation can make necessary code appear redundant on one platform, such as a `continue` before a Linux-only loop body. When appropriate, use a narrowly scoped, platform-specific lint allowance with a reason.

## Formatting

Run `tg run .#format` to format all code when you complete changes.

## Testing

The tests in `packages/tests` are the integration tests. Refer to them as the integration tests.

Write integration tests in `packages/tests/tests` by default, especially to test user-facing behavior. Write CLI tests in `packages/cli/tests` to test the CLI itself when the test needs Rust; they run `tangram` subprocesses with `tangram_tests_util::cli::Server`. Write server tests in `packages/server/tests` to test the server when the test needs Rust; they run servers in the test process with `tangram_tests_util::server::Server`. Run the CLI and server tests with nextest, which runs them under the test supervisor, so processes that outlive a test are terminated and fail the test. Use Rust unit tests for logic that does not require a running server.

Integration tests must assert user-observable behavior, not implementation details. Observe effects through the CLI or from a process inside a sandbox. Do not assert on helper processes, cgroups, log lines, database rows, on-disk layout, or counts of internal work. Tests may decode authorization tokens with `tg authorization token decode` to check the resource, permissions, and expiration that a token grants. Delete an internal check rather than replacing it, and delete a test whose only purpose is internal unless a user-visible behavior behind it is worth testing directly. Use checkpoints to drive scenarios by holding, ordering, and synchronizing server events, not to expose internal details for assertions. Never wait in an integration test with a `sleep` or a polling loop: use a blocking operation, such as a `--wait` flag, `tg sandbox status --until`, `tg process status --until`, or `tg index`, a checkpoint, or the virtual clock, and to assert that something does not happen, drive the test past the point where it would have been decided and assert once. The only exception is behavior that is defined in real time, whose `sleep` needs a comment directly above it that says why. Test an internal component in isolation with a Rust unit test when its logic is worth pinning down. If a test needs a wait only because it inspects internals, remove the inspection instead of adding the wait.

To run all tests, run `tg run .#test`. The most important tests are the integration tests in `packages/tests/tests`. You can run the integration tests with `tg run ./packages/tests#test`, which runs `nu packages/tests/test.nu` and passes along arguments after `--`; when the cloud databases are not running, the tests that require them are skipped. Do not start the test databases yourself. Rely on test databases that are already running; if none are running, allow the tests that require them to skip. Provide a regex pattern as an arg for the tests in the tests directory. For example, `tg run ./packages/tests#test -- build` will run all the build tests. To accept new and updated snapshots, run `tg run ./packages/tests#test -- --accept`.

The FoundationDB index tests and the cloud instance tests in `packages/cli/tests` and `packages/server/tests` use the cloud databases too, and skip themselves when they are not running.

To run the Rust tests of one crate, build the workspace with all features and filter by package, such as `cargo nextest run --workspace --all-features -E 'package(tangram_server_process)'`. Never use `cargo nextest run --package` or `cargo test --package`, because a crate whose backends are behind features does not compile on its own.

## macOS

The macOS app uses XcodeGen. Commit changes to `packages/macos/project.yml`, and do not commit the generated Xcode project or build directory.

Run `tg run ./packages/macos#doctor` to check the unsigned build prerequisites. Run `tg run ./packages/macos#check` for an unsigned Rust and Swift compilation that does not require an Apple Developer account. Run `tg run ./packages/macos#run` to development-sign, install, and launch the app from `~/Applications`. The `--release` flag on `scripts/macos/run.nu` selects optimized local binaries, but it does not produce a distributable release.

Run `tg run ./packages/macos#release` to archive the app, export it with Developer ID signing, notarize it, staple and verify the ticket, and create `packages/macos/build/Distribution/Tangram.zip`. Pass `-- --install` to install the verified app in `/Applications`. Set `TANGRAM_NOTARY_PROFILE` to override the default `Tangram` notarytool keychain profile.

For one-time signing setup, copy `packages/macos/Local.xcconfig.example` to `packages/macos/Local.xcconfig` and set the bundle prefix and development team. Create Apple Development and Developer ID Application certificates in the login keychain. Store the notarization credentials interactively with `xcrun notarytool store-credentials Tangram --apple-id <apple-id> --team-id <team-id>`. Do not store signing keys or notarization passwords in the repository.

## Style

### Modules and Items

- Give Rust modules focused `snake_case` names, usually consisting of a single word.
- Represent nested modules with `foo.rs` and `foo/`.
- Avoid `mod.rs`.
- Order module items as follows: one `use`, private `mod` declarations, one `pub use`, `pub mod` declarations, constants and type aliases, structs and enums, public free functions, inherent impls, trait impls, and private free functions.
- Group multiple imports into the single `use` statement, for example `use { foo::a, bar::b };`.
- Declare structs and enums in topological order, with parent types before child types.
- Order inherent impls according to the order of their struct declarations.
- Always alphabetize imports.
- Always alphabetize module declarations.
- Keep consecutive module declarations compact, without blank lines between them, even when they have attributes or comments. Keep each declaration's attributes and comments directly above it. Separate private module declarations, reexports, and public module declarations into their respective groups.
- Always alphabetize derive traits.
- Always alphabetize struct fields in definitions, literals, and destructuring patterns.
- Always alphabetize enum variants in definitions.
- Alphabetize enum variants in match arms unless guards, overlapping patterns, or catch-all patterns make order affect behavior or reachability.
- Import extension traits with `as _`.
- Separate fields, enum variants, and match arms with a blank line when any member of the group has attributes or comments. Keep each member's attributes and comments immediately above that member.
- Use consistent spacing in lists of fields, enum variants, or match arms: if any members are separated by a blank line, separate every member in that list with a blank line. Otherwise, keep the entire list compact. This applies to definitions, literals, and destructuring patterns.

### Naming

- Use `time_to_live` in code and serialized output, including compound names such as `runner_time_to_live`. Use `timeToLive` in JavaScript.
- Accept `ttl` as an input alias for `time_to_live`, including compound serialized fields and CLI flags such as `runner_ttl` and `--ttl`. Reject inputs that set both field names.
- Prefer `ttl` spellings in integration CLI arguments and configuration, except when testing canonical spellings.
- Use `ttl` in prose, reserving `time_to_live` for references to identifiers.

- Use conventional type names such as `Arg`, `Options`, `Output`, `Data`, `State`, `Builder`, etc.
- Name constructors `new` or `with_*`.
- Use bare nouns for getters.
- Use bare nouns for builder setters.
- Prefix mutating setters with `set_*`.
- Prefix a method with `try_*` when absence is returned rather than reported as an error.
- In `tangram_client` and `tangram_server`, use consistent method suffixes: `_with_instance`, `_with_arg`, `_local`, `_region` or `_regions`, `_remote` or `_remotes`, `_task`, `_inner`, `_request`, and `_with_transaction`.
- Order combined suffixes from operation modifiers to injected dependencies, for example `_with_arg_with_instance` and `_local_with_transaction`.
- Name variables with precise domain nouns.
- Use singular names for individual values and plural names for collections.
- Use conventional names for pairs, such as `sender` and `receiver`.
- Shadow a value as its representation is refined.

### Methods and Functions

- Order methods within impls top-down by call graph: place higher-level methods before the lower-level methods they call, recursively.
- Keep related method variants adjacent.
- Prefer guard clauses and `let ... else` to avoid deep nesting.
- Use `?` to propagate errors.
- Use exhaustive `match` expressions.
- Prefer binding computed field values to local variables before constructing a struct.
- Bind struct literal arguments to local variables before function or method calls, keeping the binding in the call's parent scope and using a short name such as `entry` when no more specific name is needed. Do not introduce a temporary block solely for this binding. This does not apply to enum variant construction or struct literals passed as enum variant arguments.
- Use field-init shorthand in struct expressions.
- Structure large or complex function and closure bodies as a sequence of logical phases.
- Introduce each phase with a short, active comment such as `// Parse the arg.` or `// Create the response.`.
- Separate phases with blank lines. Leave a blank line before each commented block unless it is the first block in the body; keep the comment directly adjacent to the code or attributes it describes.
- When a function or closure body contains blank lines, leave a blank line before its final return expression unless that expression is the only statement in the body.
- Prefer simple return expressions such as `Ok(output)`.
- In functions returning `Result<()>`, propagate a final fallible operation with `?` and end with `Ok(())` instead of returning the operation's result directly.
- Bind computed return values to local variables before returning them.
- Keep simple, linear bodies compact.
- Do not add phase comments to a simple body.
- Do not separate statements in a simple body with blank lines.

### Collections

- Prefer deterministic collections such as `BTreeMap` and `BTreeSet` when iteration order can affect behavior or output.
- Use hash maps and hash sets only when iteration order cannot affect behavior or output.

### Comments

- Put comments on their own lines immediately before the code they describe.
- Keep comments on a single line when practical, and do not manually wrap a single comment across multiple lines with `//` prefixes.
- Write each comment as a summary of what the code block it precedes does, which may include its purpose, such as `// Send the request while holding the lock, so that an idle peer cannot remove itself between being found and receiving the request.`.
- Do not write commentary about the code, such as its history, the alternatives that were not chosen, or remarks that do not describe what the block does.
- Include articles in comments.
- Write comments as complete sentences ending in periods.
- Avoid contractions in comments.
- Precede each non-obvious `unsafe` block with a `// SAFETY:` comment that explains the invariant that makes the block sound.

### Errors

- In code that uses the Tangram error type, return errors with `tg::Result`.
- In code that uses the Tangram error type, construct errors with `tg::error!`.
- Preserve error sources.
- Add error context at subsystem boundaries.
- Start error messages with lowercase letters.
- Write error messages as sentence fragments.
- Include articles in error messages.

### Async Code

- Clone owned values immediately before `async move`.
- Keep spawned task bodies small.
- Prefer `.boxed()` over `Box::pin` when boxing futures.
- Preserve ordering only when an operation requires it.

### Database Transactions

- In `tangram_server`, run every database transaction through a database `.run` retry loop; use `crate::database::Database::run` or `run_with_options` in application code and the backend database's `run` method during initialization or migration. Do not acquire a connection or begin a transaction directly.
- In application code, use `run` for transactions that write and `run_with_options(db::ConnectionOptions::default(), ...)` for read-only transactions.
- Keep retry closures thin: clone captured values inside the closure immediately before `async move`, then delegate the transaction body to a named helper directly beneath the calling function.
- Suffix application transaction helpers with `_with_transaction` and return `tg::Result<ControlFlow<T, crate::database::Error>>`; backend-specific helpers use the corresponding backend error type.
- Inside transaction helpers, bind every fallible database operation to a result and pass it through `crate::database::retry!` before inspecting or returning its value. This includes errors produced while reading rows from a stream.
- When calling another transaction helper, explicitly match its `ControlFlow`: unwrap `Break`, and immediately return `Continue`. Never discard the returned `ControlFlow` with `.await?;`.
- Return `ControlFlow::Break` only when the transaction attempt is complete. A retryable database error must remain a `crate::database::Error` in `ControlFlow::Continue` so the complete transaction can be retried.
- Do not add error conversions that turn `crate::database::Error` into `tg::Error`; they can bypass retry handling.

### APIs

- Mark pure constructors, accessors, and builders with `#[must_use]`.
- Keep trait impls thin.
- Place trait impls after the inherent methods they delegate to.
- Keep corresponding operation names aligned between `tangram_client` and `tangram_server`.
- In `tangram_server`, keep HTTP request handlers thin.
- In `tangram_server`, place HTTP request handlers after the underlying domain methods.

### Process and Sandbox Methods

- Each process and each sandbox boot has one holder, a scheduler or a runner, which changes its state directly and immediately from its own memory. Say "holder", not "owner", which names the principal that owns a resource.
- The index receives the holders' writes late and in any order, and applies them the same way in every order, with one pure merge per record type in `tangram_index` that every index backend shares. Test the merges with every ordering of a set of writes.
- The index decides only where there is no live holder: the start of a stopped sandbox, the destroy of a stopped sandbox, and the takeover of a dead runner's boot. Each is one conditional index write that its condition decides and that needs no sequence. Change anything else that the index holds only through a holder's write.
- A sandbox's holder writes are ordered by `(boot, sequence)`, and `sequence` starts at zero for each boot. A process's holder writes are ordered by `sequence` alone, and its `boot`, which only its holder sets and which never changes, names the boot of its sandbox that it belongs to. A handoff passes the sequence along: the scheduler's writes come first, then its handoff write, then the runner's writes.
- The first end to arrive is final, for a boot and for a process, and the sequence only orders the writes before the end. A destroyed sandbox is final.
- A takeover's conditional write records in the sandbox's record that a takeover ended the boot. A process write that leaves a process unfinished reads its sandbox's record, and the index records the process as finished only if a takeover ended the process's boot, because a live holder finishes its own processes. The read is skipped when the write finishes the process or the index already has it finished.
- Permissions merge as a union: a holder's write that carries permissions always adds them, even when the rest of the write loses. Another principal's write grants its permissions only if the record is new or the write carries its complete data. Creator, owner, sandbox, and boot are write-once.
- `runner` on a sandbox names the runner where the sandbox lives, and stays set while the sandbox is stopped. A boot that the index records on a runner never moves to another runner.
- A runner belongs to the scheduler that it first registers with for its whole life, which the index enforces. A sandbox belongs to the scheduler that first queued it for its whole life, even before it has a runner, which its record remembers with a write-once field. A scheduler is never taken over: it restarts with the same identity and resumes its saved queue, and offers nothing, including boots enqueued during its recovery, until each of its runners has reported or been declared dead. The scheduler numbers each runner connection in memory as its `connection`; no runner registration is recorded in the index. A takeover for a connection ends nothing once a newer connection of the runner registers, which checks the runner's current connection in the scheduler's memory before each of its writes.
- A create gives a boot its first holder by enqueueing it to a scheduler, whose first write carries the record and its permissions together. A start of a stopped sandbox goes to the sandbox's scheduler, and to no other scheduler, which makes the start's conditional write ("stopped at boot N becomes boot N+1 at sequence zero") together with the boot's entry in its queue, and holds the boot only once the write applies, so that of several starts only one creates the boot. A destroy of a stopped sandbox is the conditional write "stopped at boot N becomes destroying", after which no start applies, and the ttl expiry of a stopped sandbox is the same destroy, which the runner records before it removes the directory.
- The scheduler hands a boot to a runner with an offer, which it sends again until the runner answers yes or no or is declared dead. On yes the runner holds the boot, and the scheduler's handoff write, which records the runner and removes the boot from its queue, is its last write of the boot. On no the scheduler still holds the boot and offers it to another runner. A scheduler never offers a boot to a second runner while the first may have said yes: if the runner is declared dead with the offer unanswered, the scheduler ends the boot as failed, and a runner that registers again reports every boot that it holds, which the scheduler treats as its yes. A runner remembers its replies to offers for longer than the scheduler's ttl for a runner.
- A change never rides with an offer. A stop of a queued boot ends it, a request that changes a boot whose offer is unanswered gets the retry `NotReady`, after a yes the runner carries the request out, and after a no the scheduler does.
- After a handoff the scheduler still knows the sandbox, for deduplication, the placement of children, and spawns. Every spawn into a sandbox goes to the sandbox's scheduler through the holder request helper, except on the shortcut path, where a server's own runner starts the process itself and that server writes the process's first record, including a child spawned into its parent's sandbox on that runner. A runner reports each sandbox that it starts on the shortcut path to its scheduler over its control stream, without waiting for the report on the shortcut's path, and replies to another server's spawn into the sandbox with the retry `NotReady` until the scheduler acknowledges the report, and with the retry `NotHolder` afterward, so that the scheduler answers the spawn. A runner never answers another server's spawn by itself. The scheduler writes the process's queued record with its permissions at sequence zero and the boot, and sends the spawn to the runner with the boot, and the runner refuses a spawn for a boot that it is not running. If the runner refuses the spawn or does not answer, the scheduler finishes the process as failed. A process queued with a new sandbox travels inside the boot's offer. A child spawned from inside a sandbox is written by its runner with the parent as its creator.
- A live holder finishes what it holds from its own memory, and nobody finishes a process by reading the index, except a takeover. Ending a boot finishes exactly that boot's unfinished processes. When a boot fails on a runner, including before it connects, the runner finishes as failed every process that it accepted for the boot and that did not finish, from its memory. A runner whose sandbox's home is remote also records the failure in its server's local copies of the sandbox and those processes, so that no local record stays unfinished. A runner that shuts down gracefully ends its own work, and only then sends `End`.
- A takeover is the one conditional write "end boot N", which applies only while the index has the sandbox at boot N, not ended, on runner R, and which leaves the sandbox stopped, unless the boot has no stopped state, such as a sandbox with a zero ttl. Only R's scheduler issues it, for a runner that registers again without reporting the boot, for a runner whose ttl expires, and for a single server's runners when it starts, after it awaits the indexing of the writes enqueued before. It skips the boots that the runner reports and the ones in the scheduler's saved queue. A deletion of a runner is a request to the runner's scheduler, which declares the runner dead and takes over its boots as on the expiry of its ttl, so that only a runner's scheduler issues a takeover.
- On reconnect a runner reports what it runs and its directories, and the scheduler tells it which directories to keep. A stopped sandbox whose runner returns without its directory is destroyed. The scheduler owns the removal of a destroying sandbox's directory: it tells the runner, and records the sandbox as destroyed once the runner confirms or is declared dead, and a runner that reconnects with the directory of a destroying or destroyed sandbox is told to remove it.
- A method that reads or writes live state is holder-routed. Name its functions `try_X`, `try_X_runner`, `try_X_local`, `try_X_regions`, and `try_X_remotes`, such as `try_get_process` and `try_post_process_signal`.
- In `try_X`, act through this server's runner in `try_X_runner` if it holds the item, then call `try_X_local` without a search, then call `try_X_local` with a search alongside `try_X_regions` followed by `try_X_remotes`.
- In `try_X_local`, authorize, then call `request_process_holder` or `request_sandbox_holder`, which sends the request to the holder while it reads the index, and returns the holder's reply or the index's answer. Do not write another way to reach a holder.
- Keep the two helpers congruent. Their shared logic is `tangram_server_core::holder::request`, and each implements `tangram_server_core::holder::Item`.
- A read lets the index answer when the holder does not reply. A write waits for the holder, and treats a terminal state as nothing left to do.
- A holder answers through `serve_process_holder` or `serve_sandbox_holder`. A sandbox's scheduler and its runner each serve its requests and remember their own replies. A holder's response is a `tg::control::Response`, which carries an output, an error, or a `tg::control::Retry` that asks the sender to send the request again, and `into_result` decodes it to a `tg::Result<ControlFlow<T, Retry>>`. A server that does not hold the item replies with the retry `NotHolder`, and a holder that cannot act on a request yet replies with the retry `NotReady`. The shared sender handles both retries in one place: it waits for a holder's answer or a backoff and then sends the request again, without spending a retry. The holder helper sends every request, read or write, at once without waiting for the index, and sends it again with the same ID on each status wakeup until a holder acknowledges it.
- Every call of the holder helper passes the item's status wakeups, which the helper requires, so that no request is sent to a holder only once. A read that does not follow the item subscribes to the item's status for the length of the read.
- A status stream, a wait, a children stream, and a processes stream reach the holder through the data stream, such as `create_process_data_stream_local`, which passes its wakeups to the holder helper. The helper is the only code that acts on a wakeup during a request, and it reports a wakeup in `woken`, so that the stream reads again instead of waiting for the next wakeup. Do not race a call of the helper against the wakeups.
- Every request to a holder must be safe to send twice. `try_X` creates one request ID for the call, which its passes without and with a search and every resend share, and the holders remember recent replies by request ID, so that a duplicate is answered from memory and a duplicate of a request in flight is dropped. A reply that asks for a retry is not remembered.
- A method that only reads or writes the index has no holder step, such as metadata, availability, touch, and put. Lists and searches read the index and may lag.
- A method that moves a sequence of data to or from live state is a holder stream, such as a stdio read and a stdio write, while a method with one request and one reply is a holder request. A holder stream opens at the holder, which accepts the open while others reply that they are not the holder; streams with acknowledgments; reopens from the last acknowledged position after a handoff or a failure, so that no byte is delivered twice or lost; and ends on a terminal state, after which a read continues from the stored log and a write has nothing left to do.
- Open and reopen a process's holder stream with `stream_process_holder`, whose loop is `tangram_server_core::holder::stream`, and implement `StreamMethod` for the method's data. The holder answers the open through `serve_process_holder`. Do not write another way to open a relay at a holder.
- An attach and a stdio write try one location at a time, because each opens a connection that only one location may hold.

### Compatibility

- Treat Tangram as prerelease software.
- Do not add backwards compatibility when changing database schemas, API contracts, or configuration formats.
- Do not add migrations.
- Leave schema versions at 1.

### Serialization

- Keep JSON and Tangram serialization semantically congruent: both representations must preserve the same value and enum variant, but their framing may differ.
- Serialize Tangram enums with stable numeric variant IDs unless the type deliberately delegates to a canonical non-enum representation.
- Serialize kinds with stable numeric variant IDs in Tangram for compactness, even when their JSON representation uses string names.
- Use `display` and `from_str` only for types canonically defined by a stable string grammar rather than closed semantic enums.
- Use `into` and `try_from` only for lossless wrappers or refinements of another wire type.
- Use untagged Tangram enums only for canonical wire unions with provably disjoint representations; do not use them to infer a semantic enum variant from incidental payload shapes.
- Use untagged JSON enums only when their representations are disjoint, and reject unknown fields in object variants used for discrimination.
