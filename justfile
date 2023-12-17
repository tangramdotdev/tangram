check:
	cargo clippy --all
	npm run --workspaces --if-present check

clean:
	umount ~/.tangram/artifacts; rm -rf ~/.tangram

fmt:
	cargo fmt --all
	npm run --workspaces --if-present fmt

orb_clean:
	orb sh -c "umount /home/$USER/.tangram/artifacts; rm -rf /home/$USER/.tangram;"

orb_serve_dev:
	cargo build --target aarch64-unknown-linux-gnu && orb sh -c "./target/aarch64-unknown-linux-gnu/debug/tg server run"

orb_serve_release:
	cargo build --target aarch64-unknown-linux-gnu --release && orb sh -c "./target/aarch64-unknown-linux-gnu/release/tg server run"

orb_tg +ARGS:
	cargo build --target aarch64-unknown-linux-gnu && orb sh -c "./target/aarch64-unknown-linux-gnu/debug/tg {{ARGS}}"

orb_tgr +ARGS:
	cargo build --target aarch64-unknown-linux-gnu --release && orb sh -c "./target/aarch64-unknown-linux-gnu/release/tg {{ARGS}}"

serve_dev:
	cargo run -- server run

serve_release:
	cargo run --release -- server run

tg +ARGS:
	cargo run -- {{ARGS}}

tgr +ARGS:
	cargo run --release -- {{ARGS}}
