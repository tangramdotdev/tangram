# Agents

Tangram is a build system and package manager that makes builds and tests fast and reliable.

## Checking

Run `bun run check` to check changes for errors. Never use `cargo clippy --package` or `cargo check --package` as they do not check all crates. Always use `bun run check` which runs clippy on the entire workspace. Clear clippy warnings when you complete tasks.

## Formatting

Run `bun run format` to format all code when you complete changes.

## Testing

To run all tests, run `bun run test`. The most important tests are the CLI tests in `packages/cli/tests`. You can run the CLI tests with `nu packages/cli/test.nu`, and provide a regex pattern as an arg for the tests in the tests directory. For example, `nu packages/cli/test.nu build` will run all the build tests. To accept new and updated snapshots, run the tests with `--accept`.

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
- Always alphabetize derive traits.
- Always alphabetize struct fields in definitions, literals, and destructuring patterns.
- Always alphabetize enum variants in definitions.
- Alphabetize enum variants in match arms unless guards, overlapping patterns, or catch-all patterns make order affect behavior or reachability.
- Import extension traits with `as _`.

### Naming

- Use conventional type names such as `Arg`, `Options`, `Output`, `Data`, `State`, `Builder`, etc.
- Name constructors `new` or `with_*`.
- Use bare nouns for getters.
- Use bare nouns for builder setters.
- Prefix mutating setters with `set_*`.
- Prefix a method with `try_*` when absence is returned rather than reported as an error.
- In `tangram_client` and `tangram_server`, use consistent method suffixes: `_with_handle`, `_with_arg`, `_local`, `_region` or `_regions`, `_remote` or `_remotes`, `_task`, `_inner`, `_request`, and `_with_transaction`.
- Order combined suffixes from operation modifiers to injected dependencies, for example `_with_arg_with_handle` and `_local_with_transaction`.
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
- Use field-init shorthand in struct expressions.
- Structure complex bodies as a sequence of logical phases.
- Introduce each phase with a short, active comment such as `// Parse the arg.` or `// Create the response.`.
- Separate phases with blank lines.
- Leave a blank line before the final return expression in a complex body.
- Prefer simple return expressions such as `Ok(output)`.
- Bind computed return values to local variables before returning them.
- Keep simple, linear bodies compact.
- Do not add phase comments to a simple body.
- Do not separate statements in a simple body with blank lines.

### Collections

- Prefer deterministic collections such as `BTreeMap` and `BTreeSet` when iteration order can affect behavior or output.
- Use hash maps and hash sets only when iteration order cannot affect behavior or output.

### Comments

- Put comments on their own lines immediately before the code they describe.
- Use comments to explain intent, invariants, or non-obvious constraints rather than restating the code.
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
- Preserve ordering only when an operation requires it.

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
