#!/bin/sh

# Run a test under the test supervisor, which contains every process that the test starts, and fail the test if any process outlived it.
temp=$(mktemp -d "${TMPDIR:-/tmp}/tangram_test_XXXXXX") || exit 1
TMPDIR=$temp TANGRAM_TEST_SEMAPHORES=$temp/posix_semaphores "$TANGRAM_TESTS_SUPERVISOR" run --leaks "$temp/leaks" --path "$temp" -- "$@"
code=$?
if [ -s "$temp/leaks" ]; then
	echo "terminated test processes that survived cleanup:" >&2
	cat "$temp/leaks" >&2
	if [ "$code" -eq 0 ]; then
		code=1
	fi
fi

# Unlink the semaphores that the test's servers recorded or left behind, and remove the temporary directory.
"$TANGRAM_TESTS_RUNNER" semaphore cleanup "$temp" || code=1
chmod -R u+w "$temp" 2>/dev/null
rm -rf "$temp"
exit "$code"
