#!/bin/sh
# Install the latest ushr release binaries — no Go toolchain, no checkout.
#
#   curl -fsSL https://get.ushr.io | sh
#
# Downloads via get.ushr.io by default (no token needed). Set GITHUB_TOKEN to
# fetch straight from the GitHub API instead. Override the install dir with
# USHR_PREFIX (default ~/.local/bin).
set -eu

REPO="paddo-tech/ushr"
DL_BASE="${USHR_DL_BASE:-https://get.ushr.io/dl}"
PREFIX="${USHR_PREFIX:-$HOME/.local/bin}"

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$os" in
	darwin | linux) ;;
	*)
		echo "!! unsupported OS: $os" >&2
		exit 1
		;;
esac
arch=$(uname -m)
case "$arch" in
	x86_64) arch=amd64 ;;
	aarch64 | arm64) arch=arm64 ;;
	*)
		echo "!! unsupported arch: $arch" >&2
		exit 1
		;;
esac

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

sha256() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | cut -d' ' -f1
	else
		shasum -a 256 "$1" | cut -d' ' -f1
	fi
}

# Refuse to install unless the goreleaser sha256 matches; authenticity still
# rests on TLS to GitHub (both files share one trust root).
# rc 1 = mismatch (retriable race); rc 2 = no entry for this platform (final).
verify_tarball() {
	want=$(grep "_${os}_${arch}\.tar\.gz\$" "$tmp/checksums.txt" | head -1 | cut -d' ' -f1)
	if [ -z "$want" ]; then
		echo "!! No checksum entry for ${os}/${arch} in checksums.txt." >&2
		return 2
	fi
	got=$(sha256 "$tmp/ushr.tar.gz")
	if [ "$want" != "$got" ]; then
		echo "!! Checksum mismatch: expected $want, got $got." >&2
		return 1
	fi
}

if [ -n "${GITHUB_TOKEN:-}" ]; then
	echo "==> Finding latest release (GitHub API)"
	json=$(curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" \
		"https://api.github.com/repos/$REPO/releases/latest") || {
		echo "!! Could not read releases for $REPO with GITHUB_TOKEN." >&2
		exit 1
	}
	tag=$(printf '%s\n' "$json" | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)
	name="ushr_${tag#v}_${os}_${arch}.tar.gz"
	# Asset objects list "id" before "name"; remember the last id seen and emit
	# it when the wanted name matches. Downloading by asset id (octet-stream)
	# is the form that also works on private repos.
	find_asset() { # name -> id
		printf '%s\n' "$json" | awk -v name="$1" '
			/^[[:space:]]*"id":/ { id=$2; gsub(/[^0-9]/, "", id) }
			index($0, "\"name\": \"" name "\"") { print id; exit }'
	}
	asset_id=$(find_asset "$name")
	sums_id=$(find_asset "checksums.txt")
	if [ -z "$asset_id" ] || [ -z "$sums_id" ]; then
		echo "!! Missing $name or checksums.txt in release $tag." >&2
		exit 1
	fi
	echo "==> Downloading ushr $tag ($os/$arch)"
	dl_asset() { # id dest
		curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" \
			-H "Accept: application/octet-stream" \
			-o "$2" "https://api.github.com/repos/$REPO/releases/assets/$1"
	}
	dl_asset "$asset_id" "$tmp/ushr.tar.gz"
	dl_asset "$sums_id" "$tmp/checksums.txt"
	verify_tarball || {
		echo "!! Refusing to install." >&2
		exit 1
	}
else
	echo "==> Downloading ushr ($os/$arch)"
	# The tarball and checksums resolve "latest" independently server-side; a
	# release published between the two fetches makes them mismatch. One
	# retry gets a coherent pair.
	attempt=1
	while :; do
		{
			curl -fsSL -o "$tmp/ushr.tar.gz" "$DL_BASE/${os}_${arch}" &&
				curl -fsSL -o "$tmp/checksums.txt" "$DL_BASE/checksums"
		} || {
			echo "!! Download failed. If no release is published yet, build from" >&2
			echo "   source instead: clone $REPO and run deploy/install.sh." >&2
			exit 1
		}
		rc=0
		verify_tarball || rc=$?
		if [ "$rc" -eq 0 ]; then
			break
		fi
		if [ "$rc" -ne 1 ] || [ "$attempt" -ge 2 ]; then
			echo "!! Refusing to install." >&2
			exit 1
		fi
		attempt=2
		echo "==> Retrying once (a release may have been published mid-download)"
	done
fi
tar -xzf "$tmp/ushr.tar.gz" -C "$tmp"

mkdir -p "$PREFIX"
for bin in ushr ushr-agent ushr-controller; do
	install -m 755 "$tmp/$bin" "$PREFIX/$bin"
done
echo "==> Installed $("$PREFIX/ushr" version 2>/dev/null || echo ushr) to $PREFIX"

case ":$PATH:" in
	*":$PREFIX:"*) ;;
	*) echo "!! Add $PREFIX to your PATH." ;;
esac

cat <<EOF

Next — one command:

  ushr login                 # hosted plane: guided setup end to end
  ushr setup --org YOUR_ORG  # local/OSS: GitHub App + local services
EOF
