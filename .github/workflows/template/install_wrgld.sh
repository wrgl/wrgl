#!/bin/bash

# This script installs wrgld on your Linux or macOS computer.
# It should be run as root, and can be run directly from GitHub,
# for example as:
#
#   sudo bash -c 'curl -L https://github.com/wrgl/wrgl/releases/latest/download/install_wrgld.sh | bash'
#
# All downloads occur over HTTPS from the Github releases page.

if test -z "$BASH_VERSION"; then
  echo "Please run this script using bash, not sh or any other shell." >&2
  exit 1
fi

_() {

set -euo pipefail

WRGL_VERSION=%%version%%
RELEASES_BASE_URL=https://github.com/wrgl/wrgl/releases/download/"$WRGL_VERSION"
INSTALL_URL=$RELEASES_BASE_URL/install.sh

CURL_USER_AGENT=${CURL_USER_AGENT:-wrgld-installer}

OS=
ARCH=
WORK_DIR=

PLATFORM_TUPLE=

error() {
  if [ $# != 0 ]; then
    echo -e "\e[0;31m""$@""\e[0m" >&2
  fi
}

fail() {
  local error_code="$1"
  shift
  echo "*** INSTALLATION FAILED ***" >&2
  echo "" >&2
  error "$@"
  echo "" >&2
  exit 1
}

assert_linux_or_macos() {
  OS=`uname`
  ARCH=`uname -m`
  if [ "$OS" == Linux ]; then
    PLATFORM_TUPLE=linux
  elif [ "$OS" == Darwin ]; then
    PLATFORM_TUPLE=darwin
  else
    fail "E_UNSUPPORTED_OS" "wrgld installer only supports macOS and Linux."
  fi

  if [ "$ARCH" == x86_64 ]; then
    PLATFORM_TUPLE=$PLATFORM_TUPLE-amd64
  elif [ "$ARCH" == arm64 ]; then
    PLATFORM_TUPLE=$PLATFORM_TUPLE-arm64
  else
    fail "E_UNSUPPOSED_ARCH" "wrgld installer only supports installing wrgld on x86_64 or arm64."
  fi
}

assert_dependencies() {
  type -p curl > /dev/null || fail "E_CURL_MISSING" "Please install curl(1)."
  type -p tar > /dev/null || fail "E_TAR_MISSING" "Please install tar(1)."
  type -p uname > /dev/null || fail "E_UNAME_MISSING" "Please install uname(1)."
  type -p install > /dev/null || fail "E_INSTALL_MISSING" "Please install install(1)."
  type -p mktemp > /dev/null || fail "E_MKTEMP_MISSING" "Please install mktemp(1)."
}

assert_uid_zero() {
  uid=`id -u`
  if [ "$uid" != 0 ]; then
    fail "E_UID_NONZERO" "wrgld installer must run as root; please try running with sudo or running\n\`sudo bash -c 'curl -L $INSTALL_URL | bash'\`."
  fi
}

create_workdir() {
  WORK_DIR=`mktemp -d -t wrgld-installer.XXXXXX`
  cleanup() {
    rm -rf "$WORK_DIR"
  }
  trap cleanup EXIT
  cd "$WORK_DIR"
}

install_binary_release() {
  local FILE=wrgld-$PLATFORM_TUPLE.tar.gz
  local URL=$RELEASES_BASE_URL/$FILE
  echo "Downloading:" $URL
  curl -A "$CURL_USER_AGENT" -fsL "$URL" > "$FILE"
  tar zxf "$FILE"
  echo "Installing wrgld to /usr/local/bin."
  [ -d /usr/local/bin ] || install -o 0 -g 0 -d /usr/local/bin
  install -o 0 -g 0 wrgld-$PLATFORM_TUPLE/bin/wrgld /usr/local/bin
  install -o 0 -g 0 -d /usr/local/share/doc/wrgld/
  install -o 0 -g 0 -m 644 wrgld-$PLATFORM_TUPLE/LICENSE /usr/local/share/doc/wrgld/
}

assert_linux_or_macos
assert_dependencies
assert_uid_zero
create_workdir
install_binary_release

}

_ "$0" "$@"
