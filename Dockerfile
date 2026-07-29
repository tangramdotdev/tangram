FROM archlinux:base

# Install dependencies.
RUN \
pacman -Syu --noconfirm ca-certificates curl passt && \
pacman -Scc --noconfirm

# Install Cloud Hypervisor.
RUN \
test "$(uname -m)" = x86_64 && \
curl -fsSL https://github.com/tangramdotdev/bootstrap/releases/download/v2026.01.26/cloud-hypervisor_x86_64_linux > /usr/local/bin/cloud-hypervisor && \
echo "d48b7a1f418d5b7696365da8ee5a6496e4bc7c5b960702c3835b6cf011302adc  /usr/local/bin/cloud-hypervisor" | sha256sum --check && \
chmod +x /usr/local/bin/cloud-hypervisor

# Install FoundationDB.
RUN \
export VERSION=7.3.68 && \
test "$(uname -m)" = x86_64 && \
curl -fsSL https://github.com/apple/foundationdb/releases/download/${VERSION}/libfdb_c.x86_64.so > libfdb_c.so && \
mv libfdb_c.so /usr/lib/libfdb_c.so && \
ldconfig

# Install Tangram.
COPY ./target/release/tangram /usr/local/bin/tangram
RUN chmod +x /usr/local/bin/tangram

ENTRYPOINT ["tangram"]
