---
name: update
description: Update the Rust and JavaScript dependencies, including rebasing the tangramdotdev forks on their latest upstream. Use when asked to update, upgrade, or bump dependencies.
disable-model-invocation: true
---

# Update Dependencies

## 1. Rebase the forks

The workspace `Cargo.toml` depends on these forks by git URL without a branch, so Cargo uses each fork's default branch.

| Fork | Clone | Fork branch | Upstream | Upstream branch |
| --- | --- | --- | --- | --- |
| `tangramdotdev/heed` | `~/heed` | `main` | `https://github.com/meilisearch/heed.git` | `main` |
| `tangramdotdev/oxc` | `~/oxc` | `main` | `https://github.com/oxc-project/oxc.git` | `main` |
| `tangramdotdev/rquickjs` | `~/rquickjs` | `master` | `https://github.com/DelSkayn/rquickjs.git` | `master` |
| `tangramdotdev/ruff` | `~/ruff` | `main` | `https://github.com/astral-sh/ruff.git` | `main` |
| `tangramdotdev/tsrs` | `~/tsrs` | `main` | `https://github.com/oxc-project/tsrs.git` | `main` |
| `tangramdotdev/virtiofsd` | `~/virtiofsd` | `main` | `https://gitlab.com/virtio-fs/virtiofsd.git` | `main` |

Two forks are not in `Cargo.toml`, but are git submodules of other forks.

| Fork | Clone | Fork branch | Upstream | Upstream branch | Submodule of |
| --- | --- | --- | --- | --- | --- |
| `tangramdotdev/lmdb` | `~/lmdb` | `mdb.master.nested-rtxns` | `https://git.openldap.org/openldap/openldap.git` | `mdb.master` | `heed` at `lmdb-master-sys/lmdb` |
| `tangramdotdev/quickjs-ng` | `~/quickjs-ng` | `master` | `https://github.com/quickjs-ng/quickjs.git` | `master` | `rquickjs` at `sys/quickjs` |

The lmdb fork's commits include the nested read transactions patch from `meilisearch/lmdb`, so its upstream is OpenLDAP rather than `meilisearch/lmdb`, whose branch lags behind.

rquickjs commits pre-generated bindings for every target in `sys/src/bindings`, and upstream bumps quickjs without regenerating them. After bumping quickjs-ng, generate the host bindings at the old and new quickjs commits with `touch sys/build.rs && cargo build -p rquickjs-sys --features update-bindings`, diff the two, and apply only that difference to every target file. Then restore any unrelated changes the regeneration made. Check the `JS_ATOM_*` constants in particular, because removed atoms renumber the rest. Build quickjs-ng with `make` and run `make test` and `build/api-test`, which require cmake.

Rebase the submodule forks first. Then, in the parent fork, after rebasing it, point the submodule at the new commit with `git -C <submodule path> fetch origin && git -C <submodule path> checkout <new commit>`, build it, and commit the bump with a message such as `Update LMDB fork to latest upstream`. The parent fork must be pushed after the submodule fork, so its pointer resolves.

Check the tables against the `git = "https://github.com/tangramdotdev/..."` entries in `Cargo.toml` and confirm the fork branch with `gh api repos/tangramdotdev/<fork> --jq .default_branch`.

Each fork's default branch is the upstream branch plus our commits. Some of those commits also live on their own feature branches, which back our pull requests to the upstream project. A feature branch contributes to the default branch when it has commits that are not in upstream, and every one of them has a patch-equivalent commit on the default branch.

For each fork:

1. If the clone does not exist, run `git clone git@github.com:tangramdotdev/<fork>.git ~/<fork>` and `git -C ~/<fork> remote add upstream <upstream>`.
2. If the clone exists, make sure its working tree is clean and that it has the `upstream` remote. Stop and ask if there are uncommitted changes.
3. Run `git fetch --prune origin` and `git fetch upstream`, check out the fork branch, and reset it to `origin/<fork branch>` if it is behind.
4. Find the contributing feature branches. For each `origin/<branch>` other than the fork branch, take the commits that are not patch-equivalent to upstream (`git rev-list --no-merges --cherry-pick --right-only upstream/<upstream branch>...origin/<branch>`). The branch contributes when that list is not empty and every commit's `git patch-id --stable` matches a commit in `upstream/<upstream branch>..origin/<fork branch>`. Leave the other branches alone, including mirrors of upstream branches whose commits upstream has already merged.
5. Rebase each contributing branch onto upstream with `git rebase --onto upstream/<upstream branch> $(git merge-base origin/<branch> upstream/<upstream branch>) <branch>`.
6. Rebase the fork branch with `git rebase upstream/<upstream branch>`. Resolve conflicts in commits from a feature branch the same way as on the feature branch.
7. If there are conflicts, resolve them when the intent of the fork commit is clear. Otherwise, stop and ask. Submodule pointer conflicts usually mean upstream bumped the submodule while our commit points it at our fork; keep our pointer if our fork's commit contains upstream's, and otherwise ask.
8. Check that the fork branch still contains every contributing branch's commits by rerunning the `--cherry-pick` count from step 4 against the local branches. Use `git range-diff` to compare each rebased branch with its `origin/` version.
9. Build and test the rebased fork, and run its clippy, which may be stricter than when our commits were written. For a submodule fork, build it through its parent fork after bumping the pointer. tsrs tests need a `ts-ref` checkout of microsoft/TypeScript at the commit printed by `node npm/build.mjs --print-typescript-commit`, as described in its `CONTRIBUTING.md`. When upstream reorganizes code, move our commits' code to match, such as unit tests that upstream moved into integration test binaries.
10. Show the user the fork commits on top of upstream (`git log --oneline upstream/<upstream branch>..HEAD`) and the rebased feature branches. After the user confirms, push each one with `git push --force-with-lease origin <branch>`. Pushing a feature branch updates its open pull request upstream.

## 2. Update the Rust dependencies

1. Run `cargo update -v` in the repository root. This also picks up the newly pushed fork commits.
2. In the verbose output, find the `Unchanged <crate> v<current> (available: v<latest>)` lines for crates declared in `[workspace.dependencies]` in `Cargo.toml`.
3. Bump those version requirements in `Cargo.toml`. Follow the existing style: use the major version for crates at 1.0 or later, such as `"5"`, and the major and minor version for crates before 1.0, such as `"0.11"`. Keep the features and other keys unchanged.
4. Run `cargo update -v` again, and repeat until no workspace dependency has an incompatible version available, or the remaining ones cannot be updated. Leave a crate at its current version if a newer release conflicts with another dependency's requirement, and note it for the user. Keep `vhost`, `vhost-user-backend`, and `vm-memory` at the versions the virtiofsd fork uses, because tangram shares their types with it.

## 3. Update the JavaScript dependencies

Run `bun update --recursive --latest` in the repository root. This updates the root `package.json` and the workspace packages, and rewrites `bun.lock`.

## 4. Fix and verify

1. Run `bun run check` and fix the errors and warnings caused by breaking changes in the updated dependencies. If a fix needs a large refactor, such as a new ownership model in a database crate, ask the user before doing it. When upstream makes an item that tangram uses private, prefer a public replacement, such as a getter or an `_exported` method in tsrs, over making the item public in our fork.
2. Run `bun run format`.
3. Run `bun run test`.
4. Summarize for the user the forks that were rebased, the major and minor version bumps, any crates that were left behind, and the code changes the updates required.
