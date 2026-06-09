use ../../test.nu *

# Object metadata for a well formed id that does not exist fails.

let server = spawn

let output = tg object metadata fil_010000000000000000000000000000000000000000000000000000 | complete
failure $output
assert ($output.stderr | str contains 'failed to find the object metadata') "the error should mention the missing metadata"
