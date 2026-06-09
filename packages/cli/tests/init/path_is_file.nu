use ../../test.nu *

# Initializing a path that exists as a regular file fails.

let dir = mktemp --directory
"file" | save ($dir | path join "target")

let output = tg init ($dir | path join "target") | complete
failure $output
assert ($output.stderr | str contains "failed to create the directory") "the error should mention the directory"
