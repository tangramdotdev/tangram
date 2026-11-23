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

Run `tg run ./packages/macos#release -- --build-number <number>` to sign the app, notarize the disk image, and create `Tangram.dmg`, `Tangram.zip`, and the signed Sparkle appcast under `packages/macos/build/Distribution`. The disk image submission provides the tickets for both the disk image and the app in the ZIP. Pass `--install` after `--` to install the verified app in `/Applications`. Set `TANGRAM_NOTARY_PROFILE` to override the default `Tangram` notarytool keychain profile.

For one-time signing setup, copy `packages/macos/Local.xcconfig.example` to `packages/macos/Local.xcconfig` and set the bundle prefix and development team. Create Apple Development and Developer ID Application certificates in the login keychain. Store the notarization credentials interactively with `xcrun notarytool store-credentials Tangram --apple-id <apple-id> --team-id <team-id>`. Do not store private signing keys or notarization passwords in the repository.

Run `python3 scripts/release/sparkle.py bootstrap` to create or recover the production Sparkle key. The encrypted private key is stored in the private `keys-tangram-dev` bucket. Keep its passphrase in the team password vault and commit only `packages/macos/SparklePublicKey.txt`. Signed macOS packaging downloads and decrypts the key for that run.

Use an increasing `--build-number` when running `tg run .#release` with the macOS package stage, even when reusing a public version. Publish with one operator at a time. The publisher verifies the immutable, build-specific DMG before replacing the stable appcast. The stable image currently targets Apple Silicon, so publish a universal image before using the same feed for Intel clients.

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

- A sandbox is on one runner and is started in boots, boot 0 and then boot 1 after a stop and a start. A sandbox has one live boot at a time, and the status belongs to the sandbox. A process belongs to one boot, and only a process runs. A boot ends when its sandbox stops or is destroyed, and a process ends when it finishes. A sandbox is destroyed only after its last boot has ended, and stays destroyed. A boot is never destroyed. The index is the shared record, and it lags.
- Each boot and each process has at most one holder, which is the sandbox's scheduler while the item is queued and then the runner that accepts it. Say "holder", not "owner", which names the principal that owns a resource. Only the holder changes a boot or a process. It acts at once from its memory, and the index learns of the change afterwards, from the holder's write or through a control request that the holder sends to a server. On the runner, a held boot is one `tangram_server_runner_core::sandbox::Boot`, whose `next` is the only source of the runner's numbers for the boot.
- An end is final and covers everything inside it. The first end of a boot or a process to reach the index stands, and once a boot has ended, every process in it is over.
- A non-holder changes nothing and asks the holder. There are two exceptions, both by the sandbox's scheduler: it ends a boot when it declares the boot's runner dead, always through `end_dead_runner_boot`, and it ends a process that it spawned into an existing sandbox when the process's offer gets no yes, through `finish_unaccepted_spawn`.
- A boot has one counter, named `sequence` in code. Its holder numbers every change that it makes in the boot, to the sandbox or to a process, and the number travels with the change to the index, which orders each record's changes by number. The counter is handed over with the boot. The holder's end of a boot carries the counter's value, and anyone else's end carries zero. The index keeps each boot's end number. A process write numbered below its boot's end applies normally, and one at or above it records the process as failed. An end or a finish never needs a number to win: on equal numbers the finish stands. The sandbox record is ordered by boot, then number.
- A holder finishes every process in its boot, writes each finish as soon as the process ends, and sends every write again until a server accepts it. It writes the boot's end only after a server has accepted every finish: `Boot` gives the end's number only then, and `end_boot` is the runner's only writer of a boot's end, which it reports to its scheduler with a `BootEnded`. The index may show a process of such a boot as unfinished until its own finish arrives. A holder destroys a started sandbox with one write that ends the boot and destroys the sandbox, so that no start applies in between.
- The scheduler's end of a dead runner's boot waits until the boot's first record is written and indexed, is numbered zero, is written again until the index answers it, and leaves the sandbox stopped, and the index fails every unfinished process of the boot in the same transaction. Later arrivals are failed by their numbers. A sandbox that cannot stay stopped is then destroyed. The scheduler remembers the boot as ended only once the end applies, so that it tells a runner only of an end that the index records, and it also writes the end of a boot whose offer the runner answers with the boot's end, which leaves a boot whose end the index records as it is.
- The holder and the scheduler do not coordinate. The first end in the index is what happened, and a holder that learns that its boot ended stops and writes nothing more. The scheduler remembers the boots that it ended and tells the runner from its memory with an `AbandonBoot` for each when the runner reconnects, without waiting for the index. The runner abandons a boot only in `abandon_boot`, after which `Boot` gives no number, `serve_control`, the only handler of a boot's control stream, answers that the runner does not hold the boot, and `try_get_held_boot`, the only lookup through which the runner acts as a holder, finds nothing.
- A boot that does not exist has no holder, so the index decides with one conditional write by the sandbox's scheduler: a new sandbox, or "stopped at boot N becomes boot N+1". Destroying a stopped sandbox, or its ttl expiring, is the conditional write "stopped at boot N becomes destroying", so of a start and a destroy only one applies. A record's first write carries its permissions, and permissions merge as a union. Command, creator, owner, sandbox, scheduler, runner, and boot are write-once, and a later runner is ignored rather than rejected.
- The scheduler offers a boot to one runner with an `OfferBoot` and stops being its holder at that moment. Until the runner answers, the boot has no holder: nobody changes it, and the scheduler only sends the offer again. On yes the runner holds the boot, and on no the scheduler holds it again. If the runner is declared dead with the offer unanswered, the scheduler ends the boot, and the sandbox's runner is that runner, so that a later boot is offered only there. A change never rides with an offer.
- A process queued with a new boot travels in the boot's offer. A new process for the boot of a started sandbox is held by the scheduler until it is offered to the boot's runner, which says yes while it holds the boot, and no once the boot is over, after which the scheduler ends the process through `finish_unaccepted_spawn`. `Boot::accept` refuses a process once the boot took the number of its end or was abandoned, and `serve_spawn`, the runner's one handler of a spawn, says no for a boot that is over or for an earlier boot of the sandbox, so that the runner never says yes and then fails a process because its boot ended. The scheduler's record of such a process is numbered zero. On the shortcut path there is no offer: the runner holds the boot from the start and tells its scheduler afterwards with a `SandboxCreated`.
- A sandbox has one runner, a runner has one scheduler, and a scheduler is never replaced: it restarts with the same identity and resumes its saved queue. A runner that reconnects is sent its unanswered offers again and is told which of its boots ended. A runner that restarts is sent nothing until the scheduler has handled its restart: its boots are ended, and each unanswered offer is decided in `decide_saved_boot`, like a saved boot: it is ended if the index shows that the runner wrote for it, forgotten if its boot ended, and returned to the queue otherwise. A connection changes what the scheduler knows only in `apply_runner_connection`, once its addition succeeded, and only an applied connection counts as a sign of the runner's life.
- A restarted scheduler awaits indexing, learns from the index which runner has which boot, and declares no runner dead until it has read the index. It treats a runner's report of a boot in its saved queue as the runner's yes, and decides every other saved boot in `decide_saved_boot`. It acts on a runner's report only once it knows that it gave the runner the boot, and authorizes every report in `authorize_runner_report`. A report that the scheduler refuses is final, and a report that does not reach the scheduler is sent again.
- The index applies the holders' writes late and in any order with one pure `merge` per record type in `tangram_index`, which every index backend shares, together with the failure of a zero end's processes, and `validate` rejects a write that breaks a rule, such as unfinished process data without its boot. Test the merges with every ordering of a set of writes.
- Accepted for now: a scheduler that crashes after offering a boot and before recording it loses the record, and the work is stopped when the scheduler returns. With a second fault, a runner says yes, the scheduler crashes before any server accepted the runner's writes, and the runner is cut off past its ttl, so the restarted scheduler offers the boot to another runner, and both hold it until the first reconnects and is told to abandon it.
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
