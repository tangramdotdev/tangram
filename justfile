check:
	cargo clippy --all
	bun run --cwd packages/language check
	bun run --cwd packages/runtime check
	bun run --cwd packages/vscode check

clean:
	rm -rf node_modules target

clean_path:
	umount ~/.tangram/artifacts; rm -rf ~/.tangram

clean_path_orb:
	orb sh -c "umount /home/$USER/.tangram/artifacts; rm -rf /home/$USER/.tangram;"

format:
	cargo fmt --all
	bun run --cwd packages/language format
	bun run --cwd packages/runtime format
	bun run --cwd packages/vscode format

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

test *ARGS:
	cargo test --workspace {{ARGS}}

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
