{ pkgs ? ((import <nixpkgs> {}).pkgs), compiler ? "default" }:

let
  stdenv = pkgs.stdenv;


  ghcPackages = pkgs.haskellPackages.override {
    overrides = self: super: {
      mkDerivation = args: super.mkDerivation (args // {
        enableLibraryProfiling = true;
      });
    };
  };
  extsort = stdenv.mkDerivation rec {
   name = "extsort-env";
   version = "0.0.0.1";
   src = builtins.filterSource (path: type: (baseNameOf path != ".git" && baseNameOf path  != ".cabal-sandbox" && baseNameOf path != "dist" && baseNameOf path != "random") || type != "directory") ./.;
   buildInputs = [
     (ghcPackages.ghcWithPackages (ghc: [ ghc.bytestring ghc.attoparsec ghc.mtl ghc.resourcet ghc.attoparsec-binary ghc.vector ghc.vector-algorithms ghc.conduit-extra ghc.parallel ghc.pqueue ]))
     ];
   };

in extsort
