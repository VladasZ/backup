ci:
	typos
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo machete

lint:
	cargo clippy --workspace --all-targets -- -D warnings

fmt:
	cargo fmt --all

test:
	cargo test --all
	echo debug test: OK
	cargo test --all --release
	echo release test: OK

build:
	cargo build --release
