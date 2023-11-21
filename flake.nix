{
  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";

  outputs = {
    self,
    nixpkgs,
  }: let
    pkgs = import nixpkgs {
      system = "x86_64-linux";
    };
    radb = with pkgs;
      python3Packages.buildPythonPackage rec {
        pname = "radb";
        version = "3.0.5";
        format = "wheel";
        src = fetchPypi rec {
          inherit pname version format;
          sha256 = "sha256-YnBAE1d2FnziIGD8tclaC6QOCZdIoxKJH7V/fKQt1zo=";
          dist = python;
          python = "py3";
        };
      };
  in {
    devShells.x86_64-linux = with pkgs; {
      default = mkShell {
        buildInputs = [
          (python3.withPackages (pyp:
            with python3Packages; [
              sqlparse
              radb
              antlr4-python3-runtime
            ]))
        ];
      };
    };
  };
}
