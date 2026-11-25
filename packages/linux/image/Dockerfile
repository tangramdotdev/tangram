# syntax=docker/dockerfile:1
FROM debian:trixie-slim

ARG TANGRAM_VERSION

LABEL org.opencontainers.image.title="Tangram" \
      org.opencontainers.image.description="Tangram build system and package manager" \
      org.opencontainers.image.source="https://github.com/tangramdotdev/tangram" \
      org.opencontainers.image.url="https://tangram.dev" \
      org.opencontainers.image.version="$TANGRAM_VERSION"

# Install the runtime dependencies that the Debian package declares.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates fuse3 iptables libcap-ng0 libgcc-s1 nftables passt squashfs-tools && \
    rm -rf /var/lib/apt/lists/*

# Install the release binary, its aliases, and the packaged configuration.
COPY --chmod=755 tangram tgx /usr/bin/
RUN ln -s tangram /usr/bin/tg
COPY --chmod=644 config.json /etc/tangram/config.json

# Keep the server's state in a volume, at the same path that the Linux packages use.
VOLUME /var/lib/tangram

ENTRYPOINT ["tangram"]
CMD ["serve", "--config", "/etc/tangram/config.json", "--directory", "/var/lib/tangram"]
