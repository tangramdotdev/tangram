use ../../util/test.nu *

# Checking the status of a stopped server does not spawn it, even when start is enabled.

let directory = mktemp -d
let output = tg --start -d $directory server status | complete
success $output
assert equal ($output.stdout | from json) { status: stopped }

# A second status check still reports the server as stopped.
let output = tg --start -d $directory server status | complete
success $output
assert equal ($output.stdout | from json) { status: stopped }
