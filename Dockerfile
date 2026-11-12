# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM rust:1.99.0-trixie AS build

ARG TARGETARCH

# Accept the commit to report in the version.
ARG TANGRAM_CLI_COMMIT_HASH

# Install host tools and target libraries using the runtime's libc baseline.
RUN set -eu; \
    case "$TARGETARCH" in amd64|arm64) ;; *) exit 1 ;; esac; \
    dpkg --add-architecture "$TARGETARCH"; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates clang cmake curl git libclang-dev make perl pkg-config \
        python3 unzip xz-utils zstd \
        "libcap-ng-dev:$TARGETARCH" "libseccomp-dev:$TARGETARCH" "libssl-dev:$TARGETARCH" "liblzma-dev:$TARGETARCH" "zlib1g-dev:$TARGETARCH"; \
    if [ "$TARGETARCH" = arm64 ]; then \
        apt-get install -y --no-install-recommends g++-aarch64-linux-gnu; \
    fi; \
    rm -rf /var/lib/apt/lists/*

# Install the host JavaScript tooling used by Rust's build scripts.
ENV BUN_INSTALL=/usr/local
RUN curl -fsSL https://bun.com/install | bash -s "bun-v1.4.4"

WORKDIR /src
COPY . .

# Cross-compile Rust and native dependencies against the target's Debian libraries.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    set -eu; \
    case "$TARGETARCH" in \
        amd64) triple=x86_64-unknown-linux-gnu; multiarch=x86_64-linux-gnu ;; \
        arm64) triple=aarch64-unknown-linux-gnu; multiarch=aarch64-linux-gnu; \
            export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc; \
            export CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc; \
            export BINDGEN_EXTRA_CLANG_ARGS_aarch64_unknown_linux_gnu="-I/usr/aarch64-linux-gnu/include"; \
            export CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++; \
            export AR_aarch64_unknown_linux_gnu=aarch64-linux-gnu-ar; \
            export OBJCOPY_aarch64_unknown_linux_gnu=aarch64-linux-gnu-objcopy ;; \
    esac; \
    rustup target add "$triple"; \
    export PKG_CONFIG_ALLOW_CROSS=1; \
    if [ -z "${TANGRAM_CLI_COMMIT_HASH:-}" ]; then unset TANGRAM_CLI_COMMIT_HASH; fi; \
    export PKG_CONFIG_LIBDIR="/usr/lib/$multiarch/pkgconfig:/usr/share/pkgconfig"; \
    cargo build --locked --all-features --release --target "$triple" --jobs 4; \
    install -Dm755 "target/$triple/release/tangram" /output/tangram

FROM debian:trixie-slim

ARG TARGETARCH

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates curl fuse3 libcap-ng0 libseccomp2 libssl3t64 liblzma5 zlib1g libstdc++6 passt && \
    rm -rf /var/lib/apt/lists/*

# Install a checksummed Cloud Hypervisor binary for each supported architecture.
RUN set -eu; \
    case "$TARGETARCH" in \
        amd64) asset=cloud-hypervisor-static; sha=f9ffb9646783233d1f0fb09e77db1fca66499d1041e63bc011b90683c0d49933 ;; \
        arm64) asset=cloud-hypervisor-static-aarch64; sha=96ef17679a4a75be8eefa1956dd645a57f7974ec50565090ea9298f9bbb761ad ;; \
    esac; \
    curl -fsSL "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/v54.1/$asset" -o /usr/local/bin/cloud-hypervisor; \
    echo "$sha  /usr/local/bin/cloud-hypervisor" | sha256sum --check; \
    chmod +x /usr/local/bin/cloud-hypervisor

# Install the FoundationDB client matching the embedded Rust binding.
RUN set -eu; \
    case "$TARGETARCH" in amd64) arch=x86_64 ;; arm64) arch=aarch64 ;; esac; \
    curl -fsSL "https://github.com/apple/foundationdb/releases/download/7.3.68/libfdb_c.$arch.so" -o /usr/lib/libfdb_c.so; \
    ldconfig

COPY --from=build /output/tangram /usr/local/bin/tangram

ENTRYPOINT ["tangram"]
