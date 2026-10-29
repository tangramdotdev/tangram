# Agents

Tangram is a build system and package manager that makes builds and tests fast and reliable.

## Git

Work in the current checkout. Do not create additional Git worktrees.

Force pushing branches other than `main` is allowed. Never force push `main`.

## Checking

Run `bun run check` to check changes for errors. Never use `cargo clippy --package` or `cargo check --package` as they do not check all crates. Always use `bun run check` which runs clippy on the entire workspace. Clear clippy warnings when you complete tasks.

## Formatting

Run `bun run format` to format all code when you complete changes.

## Testing

Do not write Rust tests that create or start Tangram servers. Cover behavior that requires a running server with CLI tests in `packages/cli/tests`. Use Rust unit tests for logic that does not require a running server.

CLI tests must assert user-observable behavior, not implementation details. Observe effects through the CLI or from a process inside a sandbox. Do not assert on helper processes, cgroups, log lines, database rows, on-disk layout, or counts of internal work. Tests may decode authorization tokens with `tg authorization token decode` to check the resource, permissions, and expiration that a token grants. Delete an internal check rather than replacing it, and delete a test whose only purpose is internal unless a user-visible behavior behind it is worth testing directly. Use checkpoints to drive scenarios by holding, ordering, and synchronizing server events, not to expose internal details for assertions. Test an internal component in isolation with a Rust unit test when its logic is worth pinning down. If a test needs a wait only because it inspects internals, remove the inspection instead of adding the wait.

To run all tests, run `bun run test`. The most important tests are the CLI tests in `packages/cli/tests`. You can run the CLI tests with `bun run test:cli`, which runs `nu packages/cli/test.nu` and passes along its arguments; when the cloud databases are not running, the tests that require them are skipped. Run the cloud databases in another terminal with `bun run test:databases`. Provide a regex pattern as an arg for the tests in the tests directory. For example, `bun run test:cli build` will run all the build tests. To accept new and updated snapshots, run the tests with `--accept`.

Run `bun run test:fdb` to run the FoundationDB index tests against a temporary FoundationDB server. It requires `fdbserver` and `fdbcli` on Linux and Docker on macOS, and passes extra arguments to the test binary as a test name filter.

## macOS

The macOS app uses XcodeGen. Commit changes to `packages/macos/project.yml`, and do not commit the generated Xcode project or build directory.

Run `bun run macos:doctor` to check the unsigned build prerequisites. Run `bun run macos:check` for an unsigned Rust and Swift compilation that does not require an Apple Developer account. Run `bun run macos:run` to development-sign, install, and launch the app from `~/Applications`. The `--release` flag on `scripts/macos/run.nu` selects optimized local binaries, but it does not produce a distributable release.

Run `bun run macos:release` to archive the app, export it with Developer ID signing, notarize it, staple and verify the ticket, and create `packages/macos/build/Distribution/Tangram.zip`. Pass `--install` to install the verified app in `/Applications`. Set `TANGRAM_NOTARY_PROFILE` to override the default `Tangram` notarytool keychain profile.

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
