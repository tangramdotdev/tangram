# Releases

A release is four archives, one per host, each containing `tangram`, a `tg` symlink, and the `tgx` script:

| Archive | Target | Built on |
| --- | --- | --- |
| `tangram_aarch64-darwin.tar.gz` | `aarch64-apple-darwin` | an Apple Silicon Mac |
| `tangram_x86_64-darwin.tar.gz` | `x86_64-apple-darwin` | the same Mac, cross-compiled |
| `tangram_aarch64-linux.tar.gz` | `aarch64-unknown-linux-gnu` | an aarch64 Linux machine, in a container |
| `tangram_x86_64-linux.tar.gz` | `x86_64-unknown-linux-gnu` | an x86_64 Linux machine, in a container |

The Linux binaries are built in the AlmaLinux 9 image defined by `Dockerfile` in this directory, so they require glibc 2.34 or later wherever they run, regardless of the distribution of the build machine. They require no other shared libraries: the image links libseccomp and libcap-ng statically, and the linker drops OpenSSL because the default features never call it. `verify.nu` fails if that changes. The macOS binaries are signed with the Developer ID Application identity and notarized.

| Script | Purpose |
| --- | --- |
| `bump.nu <version>` | Set the version everywhere it is declared. |
| `build.nu [--target <triple>]` | Build `release/tangram_<host>.tar.gz` for one target. |
| `verify.nu <archive>` | Check an archive and print a report. Exits nonzero if any check fails. |

Each script is also a root export: `tg run .#releaseBuild -- --target x86_64-apple-darwin`. Run the scripts with `nu` directly on a machine that does not have Tangram installed.

## Validating a build machine

Follow this section on each machine before it becomes a release runner. It needs nothing from any other machine except this repository.

### 1. Install the prerequisites

On every machine:

| Tool | Check | Install |
| --- | --- | --- |
| Git | `git --version` | the system package manager |
| Nushell 0.117 or later | `nu --version` | <https://www.nushell.sh/book/installation.html> |
| Python 3 | `python3 --version` | the system package manager |

On Linux, the build runs in a container, so the machine needs no compilers:

| Tool | Check | Install |
| --- | --- | --- |
| Docker | `docker run --rm hello-world` as your user, without `sudo` | <https://docs.docker.com/engine/install/>, then add your user to the `docker` group |
| binutils | `readelf --version` | `apt install binutils` or `dnf install binutils` |

`docker info --format '{{.Architecture}}'` must print the architecture of the target, because the Linux targets are not cross-compiled.

On macOS:

| Tool | Check | Install |
| --- | --- | --- |
| Xcode | `xcode-select -p` prints a path inside `Xcode.app` | the App Store |
| Rust with both targets | `rustup target list --installed` lists `aarch64-apple-darwin` and `x86_64-apple-darwin` | <https://rustup.rs>, then `rustup target add x86_64-apple-darwin` |
| Bun | `bun --version` | <https://bun.com> |
| Rosetta | `arch -x86_64 /usr/bin/true` | `softwareupdate --install-rosetta` |
| Developer ID certificate | `security find-identity -v -p codesigning` lists `Developer ID Application` | create it in the Tangram Apple Developer account |
| Notary credentials | `xcrun notarytool history --keychain-profile Tangram` | `xcrun notarytool store-credentials Tangram --apple-id <apple-id> --team-id <team-id>` |

### 2. Get the source

```sh
git clone https://github.com/tangramdotdev/tangram
cd tangram
git checkout release-infrastructure
```

### 3. Build and verify

On Linux, build the target of the machine. The first build creates the builder image and takes the longest.

```sh
nu scripts/release/build.nu
nu scripts/release/verify.nu release/tangram_$(uname -m | sed s/arm64/aarch64/)-linux.tar.gz
```

On macOS, build both targets:

```sh
nu scripts/release/build.nu --target aarch64-apple-darwin
nu scripts/release/verify.nu release/tangram_aarch64-darwin.tar.gz
nu scripts/release/build.nu --target x86_64-apple-darwin
nu scripts/release/verify.nu release/tangram_x86_64-darwin.tar.gz
```

To check a macOS machine before it has the certificate and the notary credentials, pass `--skip-signing` to both scripts. A machine is not validated until it passes without that flag.

`verify.nu` checks the contents of the archive, the architecture, the signature and notarization on macOS, the required libraries and the newest glibc symbol on Linux, and then runs the binary against a scratch server: it prints its version, reports its health, runs a JavaScript build, and runs a command in a sandbox.

### 4. Check the oldest supported distributions (Linux only)

The binary must start on the oldest distributions that it claims to support:

```sh
archive=$PWD/release/tangram_$(uname -m | sed s/arm64/aarch64/)-linux.tar.gz
for image in almalinux:9 ubuntu:22.04 debian:12; do
	docker run --rm --volume $archive:/tangram.tar.gz:ro $image sh -c '
		mkdir /t && tar -xzf /tangram.tar.gz -C /t && /t/tangram --version
	' 2>&1 | tail -1
done
```

Each line must print `tangram <version>+<commit>`.

### 5. Report

Send back the complete output of each `verify.nu` run, the output of step 4, and how long the build took. If a step fails, send its output and stop; do not work around it on the machine.
