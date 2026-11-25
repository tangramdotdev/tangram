#!/bin/sh

set -eu

if [ -z "${TANGRAM_HELPER_PATH:-}" ]; then
	echo "error: the Tangram helper path is not set" >&2
	exit 1
fi

if [ ! -x "$TANGRAM_HELPER_PATH" ]; then
	echo "error: the Tangram helper executable was not found at $TANGRAM_HELPER_PATH" >&2
	exit 1
fi

destination="$TARGET_BUILD_DIR/$CONTENTS_FOLDER_PATH/Helpers/tangram"
mkdir -p "$(dirname "$destination")"
cp "$TANGRAM_HELPER_PATH" "$destination"
chmod +x "$destination"

if [ "${CODE_SIGNING_ALLOWED:-NO}" = "YES" ]; then
	if [ -z "${EXPANDED_CODE_SIGN_IDENTITY:-}" ]; then
		echo "error: a signing identity was not selected for the Tangram helper" >&2
		exit 1
	fi
	/usr/bin/codesign \
		--force \
		--options runtime \
		--entitlements "$SRCROOT/Tangram/Resources/TangramHelper.entitlements" \
		--sign "$EXPANDED_CODE_SIGN_IDENTITY" \
		"$destination"
fi
