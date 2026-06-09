use ../../test.nu *

# Initializing a nested path whose parent does not exist fails because only the final component is created.

let dir = mktemp --directory

let output = tg init ($dir | path join "a" "b") | complete
failure $output
assert ($output.stderr | str contains "failed to canonicalize the path") "the error should mention the path"
