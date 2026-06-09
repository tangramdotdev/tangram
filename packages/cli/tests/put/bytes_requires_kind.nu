use ../../test.nu *

# Putting raw bytes without an id requires the kind flag to compute the id.

let server = spawn

let output = tg put --bytes "hello" | complete
failure $output
assert ($output.stderr | str contains "kind must be set when using --bytes") "the error should mention the kind flag"
