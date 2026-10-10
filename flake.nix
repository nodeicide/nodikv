{
  description = "nodikv";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        buildInputs = with pkgs; [
          rustc
          cargo
          pkg-config
          openssl
        ];

        shellHook = ''
          echo "> Entered Nix Rust development shell!"
          rustc --version
          cargo --version
        '';
      };
    };
}
