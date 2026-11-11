<p align="center">
	<img width="200px" src="tangram.svg" title="Tangram">
</p>

# Tangram

Tangram is a build system and package manager.

- **TypeScript** makes writing builds easy with autocomplete and type checking.
- **Sandboxing** ensures builds do not have unspecified dependencies.
- **Lockfiles** covering all dependencies make builds reliable and reproducible.
- **Bundles** package software with all their dependencies in isolation from the rest of your system.
- **Rootless operation** makes it easy to install and use without elevated permissions.
- **Content addressed storage** minimizes disk use and network transfer.
- **Virtual filesystems** eliminate duplication on disk and download artifacts on demand.
- **Cross compilation** makes it easy to build for any machine with no virtualization.
- **Version constraints** allow you to precisely control the version of each dependency.
- **Granular caching** delivers fast incremental builds shared between machines.
- **Distributed execution** schedules builds on as many machines as possible.

To get started, run the install script below, or download the [latest release](https://github.com/tangramdotdev/tangram/releases/latest) and add it to `$PATH`.

```sh
curl -fsSL https://tangram.dev/install.sh | sh
```

To build and install from a local checkout, run `tg run .#install`. This installs the release binary at `~/.local/bin/tangram` and creates a `tg` symlink alongside it. Use `tg run .#installDev` to install a dev build instead. If Tangram is not installed yet, bootstrap with `nu scripts/install.nu`.

Create a file at the root of your project called `tangram.ts` with the following content:

```ts
export default () => tg.file("Hello, World!");
```

Run `tg build`.

```sh
$ tg build
fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g

$ tg cat fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g
Hello, World!
```

## As a build system

Use Tangram to build your existing projects faster and more reliably. This example builds a Rust project with the native libraries it depends on.

```ts
import openssl from "openssl";
import { cargo } from "rust";
import * as std from "std";

import source from "./packages/hello";

export default () => {
  return cargo.build({
    env: std.env(openssl()),
    source,
  });
};
```

Run `tg run` to build and run the program.

```sh
$ tg run
Hello, World!
```

## As a package manager

Use Tangram to build reproducible environments that start instantly. This example builds an environment that contains specific versions of `jq` and `ripgrep`.

```ts
import jq from "jq";
import sqlite from "sqlite";
import * as std from "std";

export default () => std.env(jq(), ripgrep());
```

Run `tg run -- sh` to build the environment and run a shell in it.

```sh
$ tg run -- sh

$ jq --version
jq-1.7.1

$ sqlite3 --version
3.43.2

$ exit
```

## Repository scripts

Use Tangram to run repository commands from within the checkout. All exports
run tools directly on the host, with inherited environment and terminal streams;
none create a sandbox. Cargo, Nushell, Python, Node, and Bun continue to implement
the underlying builds, tests, and OS operations.

The `run` subcommand is optional: `tg .#install` is equivalent to
`tg run .#install`. Global options go before the reference, and run options can
follow it, such as `tg .#check --watch`. Built-in commands and aliases take
precedence over references; use explicit `tg run` for a colliding tag name.

```nu
tg run .#check
tg run .#format
tg run .#build -- --release
tg run ./packages/tests#test -- --offline build
```

Arguments after `--` are forwarded unchanged to the underlying command. Checks
and formatting run Rust first, then the package tasks in parallel. The full
`test` export runs JavaScript, Rust, and e2e tests in order; its arguments are
forwarded to the e2e runner. Use the individual exports for runner-specific flags.

Checks and formatters label each output line with its package path relative to
`packages/`, such as `[clients/javascript]` or `[python]`. Workspace Rust and
script checks use `[rust]` and `[scripts]`. Each task prints its action and a
success or failure status with elapsed time; child output is piped to keep
parallel progress animations from colliding.
Task labels and status symbols use color when their output stream is a TTY,
unless `NO_COLOR` is set or `TERM` is `dumb`; redirected output remains plain.

`tg check` checks the selected modules and their imports, then runs the `check`
exports of only the explicitly selected packages or modules. `tg format`
formats module files recursively within selected directories, respecting ignores,
then runs only the selected packages' or modules' `format` exports. Both hooks
run directly on the host on every invocation; imported and nested modules do not
have their hooks invoked automatically.

| Root exports | Commands |
| --- | --- |
| `check`, `format` | All repository checks and formatters |
| `checkRust`, `checkPackages`, `checkScripts` | Individual check groups |
| `formatRust`, `formatPackages`, `formatScripts` | Individual formatter groups |
| `build` | Cargo workspace build with all features |
| `install`, `installDev` | Install release or dev CLI binaries and the `tg` symlink |
| `cli`, `cliRelease`, `cliSudo` | Run the local CLI in debug, release, or elevated mode |
| `run`, `runRelease` | Build and run a Tangram target with the local CLI |
| `test`, `testRust` | Full test suite or the Rust tests |
| `imageBuild`, `imagePush`, `imagePublish` | Build or publish the container image |
| `canary` | Build and publish the canary release |
| `clean` | Remove checkout state, dependencies, and build output |

Packages own their commands in local `tangram.ts` files. For example, run
`tg run ./packages/clients/javascript#build -- --profile debug` from the root,
or change into that package and run `tg run .#build -- --profile debug`.
The e2e package exports `test`, `databases`, and `clean`; the macOS package
exports `build`, `check`, `doctor`, `install`, `release`, and `run`; the
JavaScript client exports `test`; and `scripts/cloud/tangram.ts` exports
`databases`, `init`, `deinit`, `server`, and `runner` for the local cloud
environment, such as `tg run ./scripts/cloud#init`. Root exports compose
package commands but do not reexport them.

Each task sets the working directory expected by its underlying tools and adds
its package and repository `node_modules/.bin` directories to the inherited
PATH. Bun manifests remain for dependency management and JavaScript packaging;
repository commands live in Tangram.

Install and canary builds use the HEAD commit hash when source contents are clean.
With tracked edits or non-ignored untracked files, they use a single Git tree hash
of the working-tree contents instead. Computing it leaves the staging
area unchanged. Commands warn when the connected server's version differs;
automatic local-server mode also restarts the server with the installed binary.

### Container architectures

The Dockerfile builds the CLI and its native dependencies against Debian and
supports Linux amd64 and arm64 builds from an x86 Linux host. On an x86 Linux
host, install Docker Buildx and QEMU user-mode binfmt support for ARM runtime
image assembly; the Rust compilation itself uses a cross compiler.

```nu
tg run .#imageBuild -- --platform linux/arm64
```

To publish a multiarchitecture image index:

```nu
tg run .#imagePublish -- --tag ghcr.io/tangramdotdev/tangram:latest
```

Keep build and runtime libc versions aligned rather than copying a binary
linked on a rolling host into an older runtime image. ARM execution and build
workloads still need verification on native ARM hardware.

Cross-built V8 runtimes initialize the bundled JavaScript on the target instead
of embedding a snapshot generated for the build host's architecture. Native
builds continue to use the startup snapshot.
