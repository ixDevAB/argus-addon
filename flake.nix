{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nixpkgs, flake-utils, pyproject-nix, uv2nix, pyproject-build-systems, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        python = pkgs.python312;

        workspace = uv2nix.lib.workspace.loadWorkspace {
          workspaceRoot = ./argus;
        };

        overlay = workspace.mkPyprojectOverlay {
          sourcePreference = "wheel";
        };

        pythonSet = (pkgs.callPackage pyproject-nix.build.packages {
          inherit python;
        }).overrideScope (pkgs.lib.composeManyExtensions [
          pyproject-build-systems.overlays.default
          overlay
        ]);

        addonEnv = pythonSet.mkVirtualEnv "argus-addon-env" workspace.deps.all;
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            just
            uv
            watchexec
            addonEnv
          ];

          shellHook = ''
            export ARGUS_PYTHON_ENV="${addonEnv}"
            export PYTHONNOUSERSITE=1
            unset PYTHONPATH
            export PATH="${addonEnv}/bin:$PATH"
          '';
        };

        packages.addonEnv = addonEnv;
      }
    );
}
