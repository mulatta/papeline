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
          # Build deps (zlib-ng)
          pkgs.cmake
          pkgs.pkg-config
          # Tools
          pkgs.hurl
          pkgs.sops
          pkgs.just
          pkgs.protobuf
        ];
      };
    };
}
