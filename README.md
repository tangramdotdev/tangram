<p align="center">
	<img width="200px" src="tangram.svg" title="Tangram">
</p>

# Tangram

Tangram is a programmable build system and package manager. Tangram is a work in progress and will be ready to try soon!

- **TypeScript** makes writing builds easy with autocomplete and type checking.
- **Lockfiles** ensure builds are reproducible.
- **Bundles** package software with all their dependencies.
- **Content addressed storage** minimizes disk use and network transfer.
- **Virtual Filesystems** eliminate duplication and download artifacts on demand.
- **Granular caching** delivers fast incremental builds shared between machines.
- **Distributed execution** schedules builds on a single machine or thousands.

To get started, run the install script. It installs Tangram to `$HOME/.tangram/bin/tg`. No root permissions required.

```sh
curl -fsSL https://tangram.dev/install.sh | sh
```

## As a build system

Use Tangram for fast incremental builds of your projects. In this example, a Rust project is built with the native libraries it depends on.

```ts
import * as rust from "tg:rust";
import * as std from "tg:std";
import * as pkgconfig from "tg:pkgconfig";
import * as openssl from "tg:openssl";

import source from ".";

export let build = tg.target(() => {
  return rust.build({
    env: std.env(pkgconfig.build(), openssl.build()),
    source,
  });
});
```

## As a package manager

Use Tangram to build an environment with all of your dependencies pinned with a lockfile. Add a file named `tangram.ts` to the root of your project with the following content.

```ts
import * as jq from "tg:jq@1.7.1";
import * as ripgrep from "tg:ripgrep";
import * as std from "tg:std";

export default tg.target(() => std.env(jq.build(), ripgrep.build()));
```

Run `tg run . sh` to run a shell in the environment.

```sh
$ jq --version
jq-1.7.1
$ rg -V
ripgrep 14.1.0
```

A file named `tangram.lock` will be created at the root of your project which locks the versions of all your dependencies. Commit this file to source control to tie your code to the exact versions of the packages it depends on.
