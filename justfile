check:
	cargo clippy --all
	npm run --workspaces --if-present check

clean:
	rm -rf node_modules target

clean_path:
	umount ~/.tangram/artifacts; rm -rf ~/.tangram

clean_path_orb:
	orb sh -c "umount /home/$USER/.tangram/artifacts; rm -rf /home/$USER/.tangram;"

fmt:
	cargo fmt --all
	npm run --workspaces --if-present fmt

release:
	#!/bin/sh
	rm -rf release && mkdir release
	cargo build --release \
		--target aarch64-apple-darwin \
		--target aarch64-unknown-linux-gnu \
		--target x86_64-apple-darwin \
		--target x86_64-unknown-linux-gnu
	tar -czf release/tangram_aarch64-darwin.tar.gz -C target/aarch64-apple-darwin/release tg
	tar -czf release/tangram_aarch64-linux.tar.gz -C target/aarch64-unknown-linux-gnu/release tg
	tar -czf release/tangram_x86_64-darwin.tar.gz -C target/x86_64-apple-darwin/release tg
	tar -czf release/tangram_x86_64-linux.tar.gz -C target/x86_64-unknown-linux-gnu/release tg

test:
	cargo test --workspace

tg +ARGS:
	cargo run -- {{ARGS}}

tgr +ARGS:
	cargo run --release -- {{ARGS}}

tgo +ARGS:
	cargo build --target aarch64-unknown-linux-gnu
	orb sh -c "./target/aarch64-unknown-linux-gnu/debug/tg {{ARGS}}"

tgor +ARGS:
	cargo build --release --target aarch64-unknown-linux-gnu
	orb sh -c "./target/aarch64-unknown-linux-gnu/release/tg {{ARGS}}"
