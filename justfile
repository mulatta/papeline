# Development tasks for papeline

# List available recipes
default:
    @just --list

# Run all checks: clippy + tests
check: clippy test

# Lint with clippy
clippy:
    @cargo clippy --workspace

# Run all tests
test:
    @cargo test --workspace

# Build in release mode
build:
    @cargo build --release

# Format code (rust + nix + more via treefmt)
fmt:
    @treefmt

# Format check (CI)
fmt-check:
    @treefmt --fail-on-change
