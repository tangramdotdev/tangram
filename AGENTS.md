# Agents

Tangram is a build system and package manager that makes builds and tests fast and reliable.

## Git

Write commit messages in lowercase.

Work in the current checkout. Do not create additional Git worktrees.

Force pushing branches other than `main` is allowed. Never force push `main`.

## Repository scripts

Use Tangram as the interface for repository scripts: `tg run .#<export>` from the root, or `tg run ./packages/<package>#<export>` for a package. Pass script arguments after `--`, such as `tg run ./packages/tests#test -- --offline build`. These exports run tools directly on the host without sandboxing. Keep OS-specific operations in the existing Nushell, Python, and Node scripts as appropriate.

## Checking

Run `tg run .#check` to check changes for errors. Never use `cargo clippy --package` or `cargo check --package` as they do not check all crates. Always use `tg run .#check` which runs clippy on the entire workspace. Clear clippy warnings when you complete tasks.

Tangram supports Linux and macOS. When writing or changing code, account for conditional compilation so the code remains correct and warning-free on both platforms. When fixing warnings, inspect platform-specific code and preserve behavior on both platforms. Conditional compilation can make necessary code appear redundant on one platform, such as a `continue` before a Linux-only loop body. When appropriate, use a narrowly scoped, platform-specific lint allowance with a reason.

## Formatting

Run `tg run .#format` to format all code when you complete changes.

## Testing

The tests in `packages/tests` are the end-to-end (e2e) tests. Refer to them as the e2e tests.

Write e2e tests in `packages/tests/tests` by default, especially to test user-facing behavior. Write CLI tests in `packages/cli/tests` to test the CLI itself when the test needs Rust; they run `tangram` subprocesses with `tangram_tests_util::cli::Server`. Write server tests in `packages/server/tests` to test the server when the test needs Rust; they run servers in the test process with `tangram_tests_util::server::Server`. Run the CLI and server tests with nextest, which runs them under the test supervisor, so processes that outlive a test are terminated and fail the test. Use Rust unit tests for logic that does not require a running server.

E2e tests must assert user-observable behavior, not implementation details. Observe effects through the CLI or from a process inside a sandbox. Do not assert on helper processes, cgroups, log lines, database rows, on-disk layout, or counts of internal work. Tests may decode authorization tokens with `tg authorization token decode` to check the resource, permissions, and expiration that a token grants. Delete an internal check rather than replacing it, and delete a test whose only purpose is internal unless a user-visible behavior behind it is worth testing directly. Use checkpoints to drive scenarios by holding, ordering, and synchronizing server events, not to expose internal details for assertions. Test an internal component in isolation with a Rust unit test when its logic is worth pinning down. If a test needs a wait only because it inspects internals, remove the inspection instead of adding the wait.

To run all tests, run `tg run .#test`. The most important tests are the e2e tests in `packages/tests/tests`. You can run the e2e tests with `tg run ./packages/tests#test`, which runs `nu packages/tests/test.nu` and passes along arguments after `--`; when the cloud databases are not running, the tests that require them are skipped. Do not start the test databases yourself. Rely on test databases that are already running; if none are running, allow the tests that require them to skip. Provide a regex pattern as an arg for the tests in the tests directory. For example, `tg run ./packages/tests#test -- build` will run all the build tests. To accept new and updated snapshots, run `tg run ./packages/tests#test -- --accept`.

The FoundationDB index tests and the cloud instance tests in `packages/cli/tests` and `packages/server/tests` use the cloud databases too, and skip themselves when they are not running.

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

- Each process and each sandbox boot has one holder: the scheduler while it is queued, the runner while it runs, and the index once its state is terminal. Say "holder", not "owner", which names the principal that owns a resource.
- The index lags the holder, except that a terminal state in the index is final.
- A method that reads or writes live state is holder-routed. Name its functions `try_X`, `try_X_runner`, `try_X_local`, `try_X_regions`, and `try_X_remotes`, such as `try_get_process` and `try_post_process_signal`.
- In `try_X`, act through this server's runner in `try_X_runner` if it holds the item, then call `try_X_local` without a search, then call `try_X_local` with a search alongside `try_X_regions` followed by `try_X_remotes`.
- In `try_X_local`, authorize, then call `request_process_holder` or `request_sandbox_holder`, which sends the request to the holder while it reads the index, and returns the holder's reply or the index's answer. Do not write another way to reach a holder.
- Keep the two helpers congruent. Their shared logic is `tangram_server_core::holder::request`, and each implements `tangram_server_core::holder::Kind`.
- A read lets the index answer when the holder does not reply. A write waits for the holder, and treats a terminal state as nothing left to do.
- A holder answers through `serve_process_holder` or `serve_sandbox_holder`. A server that has handed the item off, or that has offered it to the next holder, replies `NotHolder`, and only a server that does not hold the item sends it. A holder that cannot act on a request until the item starts, such as the scheduler for a spawn into a queued sandbox, replies `NotReady`. Both replies defer the request, which the shared sender handles in one place: it waits for a holder's answer or a backoff and then sends the request again, without spending a retry. The holder helper sends every request, read or write, at once without waiting for the index, and sends it again with the same ID on each status wakeup until a holder acknowledges it, so that a request to a queued item reaches the runner as soon as the item starts.
- The scheduler answers every request for a queued sandbox and the lease requests for its queued process, and the runner answers for a boot from when its control connection is established until the boot ends. The scheduler replies `NotReady` to a spawn and to every request for its queued process except the lease requests, which only a runner can carry out. A stop of a queued first boot travels with the boot's offer, and the runner stops the boot once it starts unless a start reaches the runner first, so that after offering a boot the scheduler holds nothing.
- A status stream, a wait, a children stream, and a processes stream reach the holder through the data stream, such as `create_process_data_stream_local`, which calls the holder helper on each status wakeup.
- Every request to a holder must be safe to send twice. `try_X` creates one request ID for the call, which its passes without and with a search and every resend share, and `serve_process_holder` and `serve_sandbox_holder` remember recent replies by request ID, so that a duplicate is answered from memory and a duplicate of a request in flight is dropped. A reply that asks for a retry is not remembered. A runner remembers its replies to offers of boots for longer than the scheduler's time to live of the runner.
- A method that only reads or writes the index has no holder step, such as metadata, availability, touch, and put. Lists and searches read the index and may lag.
- A create gives an item its first holder by enqueueing it to a scheduler, such as a sandbox create and a spawn with a new sandbox. A spawn into an existing sandbox is a request to the sandbox's holder. A start of a stopped sandbox chooses the scheduler of the runner that has its directory, or any scheduler, and reserves its next boot with one index write that is rejected unless the sandbox is stopped at its boot, which records the starting boot and the boot's request in the scheduler's queue, so that one start creates the boot and the index alone is enough to finish it. Only then does the start enqueue the boot. A start that finds a starting boot with a request in its scheduler's queue sends the request again, which the scheduler ignores if it has the boot, and a restarted scheduler recovers the boot from its queue. A destroy of a stopped sandbox records it as destroying with the same kind of put, so that of a start and a destroy only one wins, and only then asks the runner that has its directory to remove it, retrying until the runner does. Change anything that the index holds with a conditional index write.
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
