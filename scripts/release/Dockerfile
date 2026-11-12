# syntax=docker/dockerfile:1

# Build the Linux release binaries on AlmaLinux 9, whose glibc 2.34 is the oldest that the binaries require.
FROM almalinux:9

# Install the compilers and the tools used by the build scripts.
RUN dnf install -y --allowerasing \
        binutils ca-certificates cmake curl diffutils file findutils gcc gcc-c++ git \
        make openssl-devel perl pkgconf-pkg-config python3 tar unzip xz zstd && \
    dnf install -y --enablerepo=crb gperf && \
    dnf clean all

# Build a static libseccomp.
RUN set -eu; \
    curl -fsSL https://github.com/seccomp/libseccomp/releases/download/v2.6.0/libseccomp-2.6.0.tar.gz -o /tmp/libseccomp.tar.gz; \
    echo "83b6085232d1588c379dc9b9cae47bb37407cf262e6e74993c61ba72d2a784dc  /tmp/libseccomp.tar.gz" | sha256sum --check; \
    mkdir /tmp/libseccomp; \
    tar -xzf /tmp/libseccomp.tar.gz -C /tmp/libseccomp --strip-components 1; \
    cd /tmp/libseccomp; \
    ./configure --prefix=/usr/local --disable-shared --enable-static --with-pic; \
    make -j "$(nproc)"; \
    make install; \
    rm -rf /tmp/libseccomp /tmp/libseccomp.tar.gz

# Build a static libcap-ng.
RUN set -eu; \
    curl -fsSL https://people.redhat.com/sgrubb/libcap-ng/libcap-ng-0.8.5.tar.gz -o /tmp/libcap-ng.tar.gz; \
    echo "3ba5294d1cbdfa98afaacfbc00b6af9ed2b83e8a21817185dfd844cc8c7ac6ff  /tmp/libcap-ng.tar.gz" | sha256sum --check; \
    mkdir /tmp/libcap-ng; \
    tar -xzf /tmp/libcap-ng.tar.gz -C /tmp/libcap-ng --strip-components 1; \
    cd /tmp/libcap-ng; \
    ./configure --prefix=/usr/local --disable-shared --enable-static --with-pic --without-python3; \
    make -j "$(nproc)"; \
    make install; \
    rm -rf /tmp/libcap-ng /tmp/libcap-ng.tar.gz

# Link libseccomp and libcap-ng statically.
ENV LIBCAPNG_LIB_PATH=/usr/local/lib \
    LIBCAPNG_LINK_TYPE=static \
    LIBSECCOMP_LIB_PATH=/usr/local/lib \
    LIBSECCOMP_LINK_TYPE=static

# Install Rust.
ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal

# Install the JavaScript tooling used by the build scripts.
ENV BUN_INSTALL=/usr/local
RUN curl -fsSL https://bun.com/install | bash -s "bun-v1.4.4"

WORKDIR /build
