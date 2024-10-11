#!/bin/sh
rm -rf release && mkdir release
TANGRAM_CLI_COMMIT_HASH=$(git rev-parse HEAD) \
cargo build --release \
	--target aarch64-apple-darwin \
	--target aarch64-unknown-linux-gnu \
	--target x86_64-apple-darwin \
	--target x86_64-unknown-linux-gnu
	ln -s tangram 
ln -s tangram target/aarch64-apple-darwin/release/tg
ln -s tangram target/aarch64-unknown-linux-gnu/release/tg
ln -s tangram target/x86_64-apple-darwin/release/tg
ln -s tangram target/x86_64-unknown-linux-gnu/release/tg
tar -czf release/tangram_aarch64-darwin.tar.gz -C target/aarch64-apple-darwin/release tangram tg
tar -czf release/tangram_aarch64-linux.tar.gz -C target/aarch64-unknown-linux-gnu/release tangram tg
tar -czf release/tangram_x86_64-darwin.tar.gz -C target/x86_64-apple-darwin/release tangram tg
tar -czf release/tangram_x86_64-linux.tar.gz -C target/x86_64-unknown-linux-gnu/release tangram tg
gh release upload canary release/* --clobber
git tag canary -f
git push origin tag canary -f
