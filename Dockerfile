FROM ubuntu:24.04

# Install certificates.
RUN apt -y update && apt -y install ca-certificates && update-ca-certificates

# Install dependencies.
RUN apt -y update && apt -y install curl

# Install FoundationDB.
RUN \
export VERSION=7.3.68 && \
curl -fsSL https://github.com/apple/foundationdb/releases/download/${VERSION}/libfdb_c.aarch64.so > libfdb_c.so && \
mv libfdb_c.so /usr/local/lib/libfdb_c.so && \
ldconfig

# Install Tangram.
COPY ./target/release/tangram /usr/local/bin/tangram
RUN chmod +x /usr/local/bin/tangram

ENTRYPOINT ["tangram"]
