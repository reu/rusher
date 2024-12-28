build:
	cargo build --release

format:
	cargo fmt --all

lint:
	cargo fmt --all -- --check --color always
	cargo clippy --workspace --all-features -- -D warnings

test:
	make lint
	RUST_BACKTRACE=full cargo test --workspace

publish: build
	cargo publish -p rusher-core
	cargo publish -p rusher-pubsub
	cargo publish -p rusher-server
	cargo publish -p rusher
