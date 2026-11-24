#!/bin/sh

set -eu

wrapper=$(/bin/realpath "$0")
export TANGRAM_MODE="${TANGRAM_MODE-client}"
exec "$(dirname "$wrapper")/../Helpers/tangram" "$@"
