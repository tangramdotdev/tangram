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
	cargo build --release \
		--target aarch64-apple-darwin \
		--target aarch64-unknown-linux-gnu \
		--target x86_64-apple-darwin \
		--target x86_64-unknown-linux-gnu
	for target in "aarch64-darwin aarch64-linux x86_64-darwin x86_64-linux"; do
		dir=$(mktemp -d)
		mkdir -p "${dir}/bin"
		cp "target/release/${target}/tg" "${dir}/bin/tg"
		cp -r "packages/install/shell" "${dir}/shell"
		tar -czf "tangram_${target}.tar.gz" -C "${dir}" .
	done

test:
	cargo test --workspace

tg +ARGS:
	cargo run -- {{ARGS}}

tgr +ARGS:
	cargo run --release -- {{ARGS}}

tgo +ARGS:
	cargo build --target aarch64-unknown-linux-gnu && orb sh -c "./target/aarch64-unknown-linux-gnu/debug/tg {{ARGS}}"

tgor +ARGS:
	cargo build --release --target aarch64-unknown-linux-gnu && orb sh -c "./target/aarch64-unknown-linux-gnu/release/tg {{ARGS}}"
