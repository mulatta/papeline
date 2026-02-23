{
  perSystem =
    { pkgs, ... }:
    {
      devShells.default = pkgs.mkShell {
        packages = [
          # Rust
          pkgs.cargo
          pkgs.rustc
          pkgs.rust-analyzer
          pkgs.clippy
          pkgs.rustfmt
          # Build deps (zlib-ng, openssl for native-tls)
          pkgs.cmake
          pkgs.pkg-config
          pkgs.openssl
          # Tools
          pkgs.hurl
          pkgs.sops
          pkgs.just
          pkgs.protobuf
          # Profiling
          pkgs.cargo-flamegraph
          pkgs.perf
          pkgs.samply
        ];
      };
    };
}
