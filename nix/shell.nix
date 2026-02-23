{
  perSystem =
    { pkgs, ... }:
    {
      devShells.default = pkgs.mkShell {
        packages = with pkgs; [
          # Rust
          cargo
          rustc
          rust-analyzer
          clippy
          # Build deps (zlib-ng, openssl for native-tls)
          cmake
          pkg-config
          openssl
          # Tools
          hurl
          sops
          just
          protobuf
          # Profiling
          cargo-flamegraph
          perf
          samply
          inferno
        ];
      };
    };
}
