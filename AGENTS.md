# Agents

Tangram is a build system and package manager that makes builds and tests fast and reliable.

## Checking

Run `bun run check` to check changes for errors. Never use `cargo clippy --package` or `cargo check --package` as they do not check all crates. Always use `bun run check` which runs clippy on the entire workspace. Clear clippy warnings when you complete tasks.

## Formatting

Run `bun run format` to format all code when you complete changes.

## Testing

To run all tests, run `bun run test`. The most important tests are the CLI tests in `packages/cli/tests`. You can run the CLI tests with `nu packages/cli/test.nu`, and provide a regex pattern as an arg for the tests in the tests directory. For example, `nu packages/cli/test.nu build` will run all the build tests. To accept new and updated snapshots, run the tests with `--accept`.

## Style

Please do not omit articles in error messages and comments. Comments should be complete sentences that end in periods and do not use contractions. Error messages should be sentence fragments that start with lowercase letters.

Please keep Rust modules organized in the following order: a single `use` statement like `use { foo::a, bar::b };`, then `mod` statements, the a single `pub use` statement, then `pub mod` statements, then all structs and enums, in topological order, so top parent types appear before child types. Then all inherent impls, in the struct declaration order, then trait impls.
