#!/bin/bash
# Build from a checkout and install the ushr binaries to ~/.local/bin.
# Everything else (config, prereqs, services, enrollment) is `ushr login`
# (hosted) or `ushr setup` (local/OSS). No checkout? Use deploy/get.sh.

set -euo pipefail

PREFIX="$HOME/.local"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

mkdir -p "$PREFIX/bin"

echo "==> Building binaries"
( cd "$REPO_ROOT" && make build )

echo "==> Installing binaries to $PREFIX/bin"
install -m 755 "$REPO_ROOT/bin/ushr"            "$PREFIX/bin/ushr"
install -m 755 "$REPO_ROOT/bin/ushr-controller" "$PREFIX/bin/ushr-controller"
install -m 755 "$REPO_ROOT/bin/ushr-agent"      "$PREFIX/bin/ushr-agent"

case ":$PATH:" in
  *":$PREFIX/bin:"*) ;;
  *) echo "!! Add $PREFIX/bin to your PATH." ;;
esac

cat <<EOF

==> Installed. Next — one command:

  ushr login                 # hosted plane: guided setup end to end
  ushr setup --org YOUR_ORG  # local/OSS: GitHub App + local services

  ushr doctor                # check host health any time
EOF
