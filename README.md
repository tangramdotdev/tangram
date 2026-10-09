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
