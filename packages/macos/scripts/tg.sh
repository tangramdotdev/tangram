#!/bin/sh

set -eu

wrapper=$(/bin/realpath "$0")
exec "$(dirname "$wrapper")/../Helpers/tangram" --mode client "$@"
