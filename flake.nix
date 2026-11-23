{
  description = "Round Table operator: development shell with the toolchain pinned by the Makefile";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/2786a2a72088ecf32aea5b3ec52099e6a7cff78f";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      # Keep in sync with the Makefile (GOLANGCI_LINT_VERSION, CONTROLLER_TOOLS_VERSION,
      # KUSTOMIZE_VERSION; HELM_VERSION is v3.19.0, any v3.19.x is fine) and with
      # go.mod (k8s.io/api v0.35 -> ENVTEST_K8S_VERSION 1.35). `nix flake check`
      # verifies that the binaries really report these versions.
      versions = {
        golangciLint = "2.8.0";
        helm = "3.19.5";
        controllerGen = "0.20.1";
        kustomize = "5.8.1";
        envtest = "1.35.0";
      };

      # Prebuilt upstream release binaries, pinned by hash, so the exact
      # versions are used regardless of what the nixpkgs revision ships.
      # (They are static Go binaries, nothing to patch.)
      sources = {
        x86_64-linux = {
          os = "linux";
          arch = "amd64";
          golangciLint = "sha256-cEi8ayXJUV7Qksg/n6hwnKl5N+rVLZ/zF6FDKZ7pelA=";
          helm = "sha256-oKXoxZLtPzdqwRBxXv8hRzDHQi+aRNls+YEX0riw5sA=";
          controllerGen = "sha256-VboKH3IFy1KbA8Ca8O4dN+D/MFRLE1acC/eHOXavbvM=";
          kustomize = "sha256-App/D04ZMsUqBHbPAqD9hVwLuFaUuCwzj8ZI3LU6gZ0=";
          envtest = "sha256-VxZxne8Uo/7D7SheXoxCgOYmiFQDm1BzqW6MCtr7HAI=";
        };
        aarch64-linux = {
          os = "linux";
          arch = "arm64";
          golangciLint = "sha256-Klg4jbivWrkzB5HOoOvdQQByPNBa1xhdkv66ruJy7Jo=";
          helm = "sha256-zgIUf/7m2ZO/iul6RKIunh2vC2nS1baaDIz2cGRFzPU=";
          controllerGen = "sha256-tqVfGHmxvoIvQZ7+LBiV+Bf1nJJwPBU7SMyHz2ZFLT4=";
          kustomize = "sha256-CVPqPkdvZtbd/NkR11D1FnuTZaqUkbIyY5jiif7ywUI=";
          envtest = "sha256-UFf7Re7PJGkp2naLIdMkNLjJbiKnjvbN/pEvGmeq5Fo=";
        };
        x86_64-darwin = {
          os = "darwin";
          arch = "amd64";
          golangciLint = "sha256-FkxygG0OMasZ2k8I//dIo/3FfiQYUSebZmyUXtm5GDA=";
          helm = "sha256-V/SoR8NJOCt8x0KmQ07yX4jwkooRPYz0kIS0ZIeO8Lk=";
          controllerGen = "sha256-7fgxoA1Rs8wHDuDTjNOZ3cyaihIKSleuZAbqTslztLs=";
          kustomize = "sha256-7nzwweNZKqe7ZrqCs1mTOpXn8uCzbl9T7QpFNbAX8vg=";
          envtest = "sha256-5zP3Lv/IqAdvLI64kt5K60u1TqAggoCM4+UfgPL/heI=";
        };
        aarch64-darwin = {
          os = "darwin";
          arch = "arm64";
          golangciLint = "sha256-b4l5qDEX4GB9S4X/SGOC7wQERAu/VPDm8APOq2zPZB4=";
          helm = "sha256-GV4k5Yf0I/FaeP7rqwRYPO7mgyNZhXWg6LOxG0P9Jv4=";
          controllerGen = "sha256-hJ1TRyvVyBQIXmiUKSCfQqjfwkKpG+KguQ2uxY7TYOA=";
          kustomize = "sha256-iIb4p4R05gjMgSNPcp/aGIqXZ9oj4oklgC8A7OK6sog=";
          envtest = "sha256-PGsevXRbgtrtR2BftWX3xnDIozRLV6N3qRTQE7a57vA=";
        };
      };

      toolsFor =
        pkgs:
        let
          src = sources.${pkgs.stdenv.hostPlatform.system};
          gh = "https://github.com";
          fetch =
            url: hash:
            pkgs.fetchurl { inherit url hash; };

          golangci-lint = pkgs.runCommand "golangci-lint-${versions.golangciLint}" { } ''
            mkdir -p $out/bin
            tar -xzf ${
              fetch "${gh}/golangci/golangci-lint/releases/download/v${versions.golangciLint}/golangci-lint-${versions.golangciLint}-${src.os}-${src.arch}.tar.gz" src.golangciLint
            } --strip-components=1 -C $out/bin --wildcards '*/golangci-lint'
          '';

          helm = pkgs.runCommand "helm-${versions.helm}" { } ''
            mkdir -p $out/bin
            tar -xzf ${
              fetch "https://get.helm.sh/helm-v${versions.helm}-${src.os}-${src.arch}.tar.gz" src.helm
            } --strip-components=1 -C $out/bin ${src.os}-${src.arch}/helm
          '';

          kustomize = pkgs.runCommand "kustomize-${versions.kustomize}" { } ''
            mkdir -p $out/bin
            tar -xzf ${
              fetch "${gh}/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv${versions.kustomize}/kustomize_v${versions.kustomize}_${src.os}_${src.arch}.tar.gz" src.kustomize
            } -C $out/bin kustomize
          '';

          controller-gen = pkgs.runCommand "controller-gen-${versions.controllerGen}" { } ''
            mkdir -p $out/bin
            install -m755 ${
              fetch "${gh}/kubernetes-sigs/controller-tools/releases/download/v${versions.controllerGen}/controller-gen-${src.os}-${src.arch}" src.controllerGen
            } $out/bin/controller-gen
          '';

          # kube-apiserver + etcd (+ kubectl) for envtest; the directory is
          # what KUBEBUILDER_ASSETS has to point at.
          envtest-assets = pkgs.runCommand "envtest-assets-${versions.envtest}" { } ''
            mkdir -p $out
            tar -xzf ${
              fetch "${gh}/kubernetes-sigs/controller-tools/releases/download/envtest-v${versions.envtest}/envtest-v${versions.envtest}-${src.os}-${src.arch}.tar.gz" src.envtest
            } --strip-components=2 -C $out controller-tools/envtest
          '';
        in
        {
          inherit
            golangci-lint
            helm
            kustomize
            controller-gen
            envtest-assets
            ;
        };
    in
    {
      packages = forAllSystems (
        pkgs:
        let
          t = toolsFor pkgs;
        in
        t // { default = t.envtest-assets; }
      );

      devShells = forAllSystems (
        pkgs:
        let
          t = toolsFor pkgs;
        in
        {
          default = pkgs.mkShell {
            packages = [
              # go.mod: `go 1.26.1`, toolchain go1.26.3; nixpkgs-unstable's go
              # is >= that, and the Makefile's GO_TOOLCHAIN=...+auto never
              # accepts an older one.
              pkgs.go
              t.golangci-lint
              t.helm
              t.kustomize
              t.controller-gen
              pkgs.setup-envtest
              pkgs.kubectl
              pkgs.kind
              pkgs.gnumake
              pkgs.git
              pkgs.jq
              pkgs.yq-go
              pkgs.gh
            ];

            # Used by `make test` instead of downloading assets with setup-envtest.
            KUBEBUILDER_ASSETS = "${t.envtest-assets}";
          };
        }
      );

      checks = forAllSystems (
        pkgs:
        let
          t = toolsFor pkgs;
          expect = name: bin: want: ''
            got="$(${bin})"
            case "$got" in *${want}*) ;; *) echo "${name}: want ${want}, got: $got" >&2; exit 1;; esac
          '';
        in
        {
          tool-versions = pkgs.runCommand "tool-versions" { } ''
            ${expect "golangci-lint" "${t.golangci-lint}/bin/golangci-lint version" versions.golangciLint}
            ${expect "helm" "${t.helm}/bin/helm version --short" "v${versions.helm}"}
            ${expect "kustomize" "${t.kustomize}/bin/kustomize version" "v${versions.kustomize}"}
            ${expect "controller-gen" "${t.controller-gen}/bin/controller-gen --version" "v${versions.controllerGen}"}
            test -x ${t.envtest-assets}/kube-apiserver
            test -x ${t.envtest-assets}/etcd
            touch $out
          '';
        }
      );
    };
}
