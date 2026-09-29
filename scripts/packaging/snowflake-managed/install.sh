#!/bin/sh
# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# snowflake-managed installer for Snowflake CLI.
# Releng copies this file to https://sfc-repo.snowflakecomputing.com/snowflake-cli/install.sh
#
# Layout (must match src/snowflake/cli/_plugins/upgrade/layout.py):
#   ~/.local/share/snowflake-cli/<version>/snow
#   ~/.local/share/snowflake-cli/bin/snow          # symlink, atomic replace
#
# Override install root with SNOWFLAKE_CLI_MANAGED_HOME (tests and support).
# Override repo base with SNOWFLAKE_CLI_MANAGED_REPO (tests; default is sfc-repo).

# shellcheck shell=dash
# shellcheck disable=SC3043

set -e

DOCKER_ENV=${DOCKER_ENV:-""}
SKIP_PATH_PROMPT=${SKIP_PATH_PROMPT:-""}
NON_INTERACTIVE=${NON_INTERACTIVE:-""}

if [ -f /.dockerenv ] || [ -n "$DOCKER_ENV" ]; then
    NON_INTERACTIVE=1
    SKIP_PATH_PROMPT=1
fi

BINARY_NAME="snow"
DEFAULT_REPO_BASE="https://sfc-repo.snowflakecomputing.com/snowflake-cli"
POINTER_NAME="stable_version.txt"
MANIFEST_NAME="manifest.json"
MANIFEST_SIG_NAME="manifest.json.sig"
PATH_BEGIN="# snowflake-cli snowflake-managed PATH begin"
PATH_END="# snowflake-cli snowflake-managed PATH end"

TEMP_DIR=$(mktemp -d)

STTY_SAVED=""
if [ -e /dev/tty ]; then
    STTY_SAVED=$(stty -g < /dev/tty 2>/dev/null) || STTY_SAVED=""
    if [ -n "$STTY_SAVED" ]; then
        stty -ocrnl < /dev/tty 2>/dev/null || true
    fi
fi

restore_stty() {
    if [ -n "$STTY_SAVED" ]; then
        stty "$STTY_SAVED" < /dev/tty 2>/dev/null || true
    fi
}

cleanup() {
    [ -d "$TEMP_DIR" ] && rm -rf "$TEMP_DIR"
    restore_stty
}

trap cleanup EXIT

print_success() { echo "✓ $1"; }
print_error() { echo "Error: $1" >&2; }

resolve_install_root() {
    if [ -n "$SNOWFLAKE_CLI_MANAGED_HOME" ]; then
        echo "$SNOWFLAKE_CLI_MANAGED_HOME"
    elif [ -n "$XDG_DATA_HOME" ]; then
        echo "${XDG_DATA_HOME}/snowflake-cli"
    else
        echo "${HOME}/.local/share/snowflake-cli"
    fi
}

resolve_repo_base() {
    if [ -n "$SNOWFLAKE_CLI_MANAGED_REPO" ]; then
        echo "$SNOWFLAKE_CLI_MANAGED_REPO" | sed 's:/*$::'
    else
        echo "$DEFAULT_REPO_BASE"
    fi
}

ALL_VERSIONS_INSTALL_DIR=$(resolve_install_root)
BIN_DIR="${ALL_VERSIONS_INSTALL_DIR}/bin"
REPO_BASE=$(resolve_repo_base)

# Must match src/snowflake/cli/_plugins/upgrade/layout.py validate_version:
# alphanumeric start, then [A-Za-z0-9._-], and not a reserved root name.
validate_version() {
    local version=$1
    if ! echo "$version" | grep -qE '^[A-Za-z0-9][A-Za-z0-9._-]*$'; then
        print_error "Invalid snowflake-managed version '$version'. Use a version directory name such as 3.12.0."
        exit 1
    fi
    if [ "$version" = "bin" ] || [ "$version" = ".current" ] || [ "$version" = ".previous" ]; then
        print_error "Invalid snowflake-managed version '$version'. Use a version directory name such as 3.12.0."
        exit 1
    fi
}

# Package names from the manifest are used in URLs. Reject path separators so a
# compromised name cannot walk out of the version directory (CWE-22).
validate_package_name() {
    local name=$1
    if [ -z "$name" ] || [ "$name" = "." ] || [ "$name" = ".." ]; then
        print_error "Invalid package name in manifest: $name"
        exit 1
    fi
    case "$name" in
        *[!A-Za-z0-9._+-]*)
            print_error "Invalid package name in manifest: $name"
            exit 1
            ;;
    esac
}

url_encode() {
    echo "$1" | sed 's/+/%2B/g'
}

repo_version_dir() {
    echo "${REPO_BASE}/$(url_encode "$1")/"
}

get_stable_version() {
    local version_url="${REPO_BASE}/${POINTER_NAME}"
    local version
    if ! version=$(curl -fsSL "$version_url" 2>&1); then
        print_error "Failed to fetch stable version from $version_url"
        print_error "$version"
        exit 1
    fi
    echo "$version" | tr -d '[:space:]'
}

get_platform_info() {
    local os
    local arch
    os=$(uname -s | tr '[:upper:]' '[:lower:]')
    arch=$(uname -m | tr '[:upper:]' '[:lower:]')

    case "$arch" in
        x86_64|amd64) arch="amd64" ;;
        aarch64|arm64) arch="arm64" ;;
    esac

    case "$os" in
        linux|darwin) ;;
        *)
            print_error "snowflake-managed install.sh does not support this operating system ($os). Use install.ps1 on Windows."
            exit 1
            ;;
    esac

    echo "${os} ${arch}"
}

parse_manifest_json() {
    local json_content=$1
    local os=$2
    local arch=$3

    if command -v jq > /dev/null 2>&1; then
        local tar_name
        local checksum
        tar_name=$(echo "$json_content" | jq -r ".packages.${os}.${arch}.name")
        checksum=$(echo "$json_content" | jq -r ".packages.${os}.${arch}.checksum")
        echo "${tar_name} ${checksum}"
    else
        local tar_name
        local checksum
        tar_name=$(echo "$json_content" | grep -o '"name"[[:space:]]*:[[:space:]]*"[^"]*\.tar\.gz"' | grep -o '"[^"]*\.tar\.gz"' | tr -d '"' | grep "${os}-${arch}" | head -1)
        checksum=$(echo "$json_content" | grep -A2 "\"${tar_name}\"" | grep -o '"checksum"[[:space:]]*:[[:space:]]*"[^"]*"' | cut -d'"' -f4 | head -1)
        echo "${tar_name} ${checksum}"
    fi
}

write_manifest_pubkey() {
    if [ -n "${SNOWFLAKE_CLI_MANAGED_MANIFEST_PUBKEY_FILE:-}" ]; then
        echo "$SNOWFLAKE_CLI_MANAGED_MANIFEST_PUBKEY_FILE"
        return 0
    fi
    cat > "$TEMP_DIR/managed_manifest.pub.pem" <<'EOF'
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAmecfO6u7BbYw16AHXhDy
MOGlJop5LZ08eZrsVswXlps+19CtIEJQQJZPjTKDAqDay0+TpNfj0ErberXW4pRl
WTHV1IsRrMBZQwj+Rx7wfcIBCnctMvXVinCTQxDCG6QVEIvyntSi/shju7ktWeON
GT4deCy7ZnTnE2abtK8sre+sbNAy5UbPiiAWULcoXmmpI/upQhGWDYsugMGdUQAm
aA3u/QzoH/PwH8pClZWUJfbid05U51rUV6WzScb6PZP0PK9JYcSVTHZscmGCYkY3
LrlCNZ+zMy1JCnp2B0cWzwaHoHFBmAACUMJLHz1NtQyRbHcLekKSgr5CIMMNIZmV
rDYYTrGxnqfph6RMCVBd1RuzcXSVZhxTI2o93r0GR/7J+tr1GmY003dfuuWlHsBa
+zKA8NyJDCnq7atW96AnnH78VQ+PuwMxEzmkgbi50lmAtGZuBpRtQn5ySWnaVKG1
S2qBnPgiPWLJ7r7cViK0ZL5Mk8z0hkLXBDlop9MVea9LV9x1evFDkgoC49Gn5g4Z
yqwTODl1tnE7/i47SKfqZl6lTnhbYLlUYYGt1pFmOp8w98ycCNynSFwAKKPr48FJ
uUCg4ucODyWnSw5d7xVnaUbqiHCj7QgPGWR5ssyFnlYHJD8os319nd/0xgvopeeX
nPViy3zvEUsQyDblLYH2jQMCAwEAAQ==
-----END PUBLIC KEY-----
EOF
    echo "$TEMP_DIR/managed_manifest.pub.pem"
}

verify_manifest_signature() {
    local manifest=$1
    local sig=$2
    local pubkey
    pubkey=$(write_manifest_pubkey)
    if ! openssl dgst -sha256 -verify "$pubkey" -signature "$sig" "$manifest" >/dev/null 2>&1; then
        print_error "Invalid signature on manifest.json. Refusing to install."
        return 1
    fi
    return 0
}

download_manifest() {
    local version=$1
    local manifest_url
    local sig_url
    manifest_url="$(repo_version_dir "$version")${MANIFEST_NAME}"
    sig_url="$(repo_version_dir "$version")${MANIFEST_SIG_NAME}"
    if ! curl -fsSL "$manifest_url" -o "$TEMP_DIR/manifest.json"; then
        print_error "Failed to download manifest from $manifest_url"
        return 1
    fi
    if ! curl -fsSL "$sig_url" -o "$TEMP_DIR/manifest.json.sig"; then
        print_error "Missing signature for manifest.json. Refusing to install unsigned snowflake-managed package."
        return 1
    fi
    if ! verify_manifest_signature "$TEMP_DIR/manifest.json" "$TEMP_DIR/manifest.json.sig"; then
        return 1
    fi
    cat "$TEMP_DIR/manifest.json"
}

show_spinner() {
    local pid=$1
    local message=$2
    local i=0

    while kill -0 "$pid" 2>/dev/null; do
        case $((i % 10)) in
            0) local char='⠋' ;;
            1) local char='⠙' ;;
            2) local char='⠹' ;;
            3) local char='⠸' ;;
            4) local char='⠼' ;;
            5) local char='⠴' ;;
            6) local char='⠦' ;;
            7) local char='⠧' ;;
            8) local char='⠇' ;;
            9) local char='⠏' ;;
        esac
        printf '\r%s %s' "$char" "$message"
        i=$((i + 1))
        sleep 0.1
    done
}

run_with_spinner() {
    local message=$1
    local success_message=$2
    local error_message=$3
    shift 3

    local err_file
    err_file=$(mktemp)

    "$@" > /dev/null 2>"$err_file" &
    local cmd_pid=$!
    show_spinner $cmd_pid "$message"
    wait $cmd_pid
    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        rm -f "$err_file"
        printf '\r\033[K%s\n' "$(print_success "$success_message")"
        return 0
    else
        if [ -s "$err_file" ]; then
            cat "$err_file" >&2
        fi
        rm -f "$err_file"
        printf '\r\033[K%s\n' "$(print_error "$error_message (exit code: $exit_code)")"
        return $exit_code
    fi
}

check_dependencies() {
    if ! command -v tar > /dev/null 2>&1; then
        print_error "tar is required but not installed"
        exit 1
    fi
    if ! command -v curl > /dev/null 2>&1; then
        print_error "curl is required but not installed"
        exit 1
    fi
    if ! command -v openssl > /dev/null 2>&1; then
        print_error "openssl is required to verify the snowflake-managed manifest signature"
        exit 1
    fi
}

curl_download() {
    local url=$1
    local dest=$2
    if [ -n "$NON_INTERACTIVE" ] || [ ! -t 1 ]; then
        curl -fsSL "$url" -o "$dest"
    else
        curl -fL --progress-bar "$url" -o "$dest"
    fi
}

download_package() {
    local tar_url=$1
    local rc=0
    cd "$TEMP_DIR"
    curl_download "$tar_url" package.tar.gz || rc=$?
    if [ "$rc" -eq 0 ]; then
        return 0
    fi
    if [ ! -f package.tar.gz ] || [ ! -s package.tar.gz ]; then
        echo "" >&2
        echo "Retrying download over HTTP/1.1..." >&2
        if [ -n "$NON_INTERACTIVE" ] || [ ! -t 1 ]; then
            curl -fsSL --http1.1 "$tar_url" -o package.tar.gz
        else
            curl -fL --http1.1 --progress-bar "$tar_url" -o package.tar.gz
        fi
        return $?
    fi
    return "$rc"
}

verify_checksum() {
    local file=$1
    local expected=$2
    local actual=""
    expected=$(echo "$expected" | tr '[:upper:]' '[:lower:]' | sed 's/^sha256://')

    if command -v shasum > /dev/null 2>&1; then
        actual=$(shasum -a 256 "$file" | awk '{print $1}')
    elif command -v sha256sum > /dev/null 2>&1; then
        actual=$(sha256sum "$file" | awk '{print $1}')
    else
        print_error "Neither shasum nor sha256sum command found"
        return 1
    fi
    actual=$(echo "$actual" | tr '[:upper:]' '[:lower:]')

    if [ "$actual" != "$expected" ]; then
        print_error "Checksum mismatch: expected $expected, got $actual"
        return 1
    fi
    return 0
}

unwrap_payload() {
    local extract_dir=$1
    if [ -f "$extract_dir/$BINARY_NAME" ]; then
        echo "$extract_dir"
        return 0
    fi

    local first="" extra=0 child
    for child in "$extract_dir"/* "$extract_dir"/.[!.]*; do
        [ -e "$child" ] || continue
        [ -d "$child" ] || continue
        if [ -z "$first" ]; then
            first=$child
        else
            extra=1
        fi
    done
    if [ "$extra" = "0" ] && [ -n "$first" ] && [ -f "$first/$BINARY_NAME" ]; then
        echo "$first"
        return 0
    fi

    print_error "Tarball does not contain $BINARY_NAME at the root or in a single top-level directory."
    return 1
}

extract_package() {
    local extract_dir="$TEMP_DIR/extract"
    mkdir -p "$extract_dir"
    tar -xzf "$TEMP_DIR/package.tar.gz" -C "$extract_dir"
    rm -f "$TEMP_DIR/package.tar.gz"

    local payload
    if ! payload=$(unwrap_payload "$extract_dir"); then
        return 1
    fi
    echo "$payload" > "$TEMP_DIR/payload_path"
}

download_and_extract() {
    local version=$1
    local manifest_json
    if ! manifest_json=$(download_manifest "$version"); then
        print_error "Failed to download manifest"
        exit 1
    fi

    local platform_info
    platform_info=$(get_platform_info)
    os_name=$(echo "$platform_info" | cut -d' ' -f1)
    arch=$(echo "$platform_info" | cut -d' ' -f2)

    local manifest_data
    manifest_data=$(parse_manifest_json "$manifest_json" "$os_name" "$arch")
    tar_name=$(echo "$manifest_data" | cut -d' ' -f1)
    expected_checksum=$(echo "$manifest_data" | cut -d' ' -f2)

    if [ -z "$tar_name" ] || [ -z "$expected_checksum" ] || [ "$tar_name" = "null" ] || [ "$expected_checksum" = "null" ]; then
        print_error "Snowflake CLI is not available for your platform: ${os_name}-${arch}"
        exit 1
    fi
    validate_package_name "$tar_name"

    local encoded_tar_name
    encoded_tar_name=$(url_encode "$tar_name")
    local tar_url
    tar_url="$(repo_version_dir "$version")${encoded_tar_name}"
    echo "Downloading Snowflake CLI..."
    local curl_exit=0
    download_package "$tar_url" || curl_exit=$?

    if ! verify_checksum "$TEMP_DIR/package.tar.gz" "$expected_checksum"; then
        print_error "Checksum verification failed. Refusing to install."
        exit 1
    fi

    if [ -t 1 ] && [ -z "$NON_INTERACTIVE" ]; then
        printf '\033[1A\033[2K\033[1A\033[2K'
    fi
    if [ "$curl_exit" -ne 0 ]; then
        echo "Note: curl exited with $curl_exit after the transfer; checksum matched, continuing." >&2
    fi
    print_success "Downloaded Snowflake CLI successfully"

    if ! run_with_spinner "Extracting Snowflake CLI..." "Extracted Snowflake CLI successfully" "Failed to extract Snowflake CLI" extract_package; then
        exit 1
    fi
}

atomic_symlink_replace() {
    local link_path=$1
    local target=$2
    local dir name tmp
    dir=$(dirname "$link_path")
    name=$(basename "$link_path")
    mkdir -p "$dir"
    tmp="$dir/.${name}.$$.tmp"
    if [ -e "$tmp" ] || [ -L "$tmp" ]; then
        rm -f "$tmp"
    fi
    ln -s "$target" "$tmp"
    if ! mv -f "$tmp" "$link_path"; then
        rm -f "$tmp"
        return 1
    fi
}

read_pointer() {
    local path=$1
    if [ -f "$path" ]; then
        tr -d '[:space:]' < "$path"
    fi
}

write_pointer() {
    local path=$1
    local value=$2
    printf '%s\n' "$value" > "$path"
}

gc_old_versions() {
    local keep_current=$1
    local keep_previous=$2
    [ -d "$ALL_VERSIONS_INSTALL_DIR" ] || return 0

    for child in "$ALL_VERSIONS_INSTALL_DIR"/*; do
        [ -e "$child" ] || continue
        [ -d "$child" ] || continue
        local name
        name=$(basename "$child")
        if [ "$name" = "bin" ] || [ "$name" = "$keep_current" ] || [ "$name" = "$keep_previous" ]; then
            continue
        fi
        rm -rf "$child"
    done
}

install_binary() {
    local version=$1
    local payload
    payload=$(cat "$TEMP_DIR/payload_path")
    local install_dir="${ALL_VERSIONS_INSTALL_DIR}/${version}"
    local live=""

    if [ ! -f "$payload/$BINARY_NAME" ]; then
        print_error "Binary '$BINARY_NAME' not found in extracted package"
        return 1
    fi

    if [ -L "$BIN_DIR/$BINARY_NAME" ]; then
        live=$(readlink "$BIN_DIR/$BINARY_NAME")
    fi
    if [ -n "$live" ] && [ "$(dirname "$live")" = "$install_dir" ]; then
        print_error "Refusing to overwrite the running snowflake-managed binary at $live."
        print_error "Install into a new version directory, then retarget the shim."
        return 1
    fi

    mkdir -p "$BIN_DIR"
    mkdir -p "$ALL_VERSIONS_INSTALL_DIR"

    if [ -d "$install_dir" ] || [ -L "$install_dir" ]; then
        rm -rf "$install_dir"
    fi

    mkdir -p "$install_dir"
    # Copy payload contents so a root-level tarball (snow at archive root) and a
    # wrapped directory both land as <root>/<version>/snow.
    cp -R "$payload"/. "$install_dir"/

    chmod +x "$install_dir/$BINARY_NAME"

    local previous
    previous=$(read_pointer "$ALL_VERSIONS_INSTALL_DIR/.current")
    if [ -n "$previous" ] && [ "$previous" != "$version" ]; then
        write_pointer "$ALL_VERSIONS_INSTALL_DIR/.previous" "$previous"
    fi
    write_pointer "$ALL_VERSIONS_INSTALL_DIR/.current" "$version"

    if ! atomic_symlink_replace "$BIN_DIR/$BINARY_NAME" "$install_dir/$BINARY_NAME"; then
        print_error "Failed to retarget $BIN_DIR/$BINARY_NAME"
        return 1
    fi

    gc_old_versions "$version" "$(read_pointer "$ALL_VERSIONS_INSTALL_DIR/.previous")"

    local smoke_err
    smoke_err=$(mktemp)
    if ! "$BIN_DIR/$BINARY_NAME" --version > /dev/null 2>"$smoke_err"; then
        if grep -qiE '(GLIBC|GLIBCXX|cannot open shared object|Exec format error)' "$smoke_err" 2>/dev/null; then
            print_error "Binary is not compatible with this system:"
            cat "$smoke_err" >&2
            rm -f "$smoke_err"
            return 1
        fi
    fi
    rm -f "$smoke_err"
}

check_path() {
    case ":$PATH:" in
        *":$BIN_DIR:"*) return 0 ;;
        *) return 1 ;;
    esac
}

print_session_path() {
    if check_path; then
        return
    fi
    echo ""
    echo "This session does not have $BIN_DIR on PATH. Run:"
    # shellcheck disable=SC2016
    printf '    export PATH="%s:$PATH"\n' "$BIN_DIR"
}

append_path_block() {
    local config_file=$1
    local shell_name=$2
    local config_dir
    config_dir=$(dirname "$config_file")
    if [ ! -d "$config_dir" ]; then
        mkdir -p "$config_dir"
    fi

    if [ -f "$config_file" ] && grep -F "$PATH_BEGIN" "$config_file" > /dev/null 2>&1; then
        print_success "$config_file already prepends the snowflake-managed bin dir"
        return 0
    fi

    {
        echo ""
        echo "$PATH_BEGIN"
        if [ "$shell_name" = "fish" ]; then
            echo "fish_add_path $BIN_DIR"
        else
            echo "export PATH=\"$BIN_DIR:\$PATH\""
        fi
        echo "$PATH_END"
    } >> "$config_file"
    print_success "Updated $config_file to prepend $BIN_DIR"
}

suggest_path_update() {
    print_session_path

    if check_path; then
        return
    fi

    if [ -n "$SKIP_PATH_PROMPT" ]; then
        echo "Skipping PATH configuration (SKIP_PATH_PROMPT is set)"
        return
    fi

    local shell_name
    shell_name=$(basename "${SHELL:-/bin/sh}")
    local config_file=""
    local show_manual_instructions=0

    case "$shell_name" in
        zsh) config_file="$HOME/.zshrc" ;;
        bash) config_file="${HOME}/$( [ -f "$HOME/.bash_profile" ] && echo .bash_profile || echo .bashrc )" ;;
        fish) config_file="$HOME/.config/fish/config.fish" ;;
        *) config_file="" ;;
    esac

    if [ -n "$config_file" ]; then
        if [ -n "$NON_INTERACTIVE" ]; then
            append_path_block "$config_file" "$shell_name"
            echo ""
            printf 'Run '\''source %s'\'' or restart your shell to update PATH.\n' "$config_file"
            return
        fi

        if [ -f "$config_file" ]; then
            printf '\033[93m%s is not in your PATH but must be to use Snowflake CLI. Would you like to add this? [\033[1;32my\033[0m/\033[1;31mN\033[0m]\033[0m ' "$BIN_DIR"
        else
            printf '\033[93m%s is not in your PATH but must be to use Snowflake CLI. Would you like to create %s and add this? [\033[1;32my\033[0m/\033[1;31mN\033[0m]\033[0m ' "$BIN_DIR" "$config_file"
        fi

        if [ -e /dev/tty ]; then
            read -r REPLY < /dev/tty
        else
            read -r REPLY || REPLY="N"
        fi

        if [ "$REPLY" = "y" ] || [ "$REPLY" = "Y" ]; then
            append_path_block "$config_file" "$shell_name"
            echo ""
            printf 'Run '\''source %s'\'' or restart your shell to update PATH.\n' "$config_file"
        else
            show_manual_instructions=1
        fi
    else
        show_manual_instructions=1
    fi

    if [ $show_manual_instructions -eq 1 ]; then
        echo ""
        echo "To use Snowflake CLI, add the following line to your shell configuration:"
        if [ "$shell_name" = "fish" ]; then
            echo ""
            echo "    fish_add_path $BIN_DIR"
            echo ""
        else
            echo ""
            # shellcheck disable=SC2016
            printf '    export PATH="%s:$PATH"\n' "$BIN_DIR"
            echo ""
        fi
    fi
}

main() {
    check_dependencies

    local version
    if ! version=$(get_stable_version); then
        exit 1
    fi
    if [ -z "$version" ]; then
        print_error "Failed to determine the latest snowflake-managed version of Snowflake CLI"
        exit 1
    fi
    validate_version "$version"

    download_and_extract "$version"

    if ! run_with_spinner "Installing Snowflake CLI..." "Installed Snowflake CLI v$version successfully" "Installation failed" install_binary "$version"; then
        exit 1
    fi

    suggest_path_update
}

main
