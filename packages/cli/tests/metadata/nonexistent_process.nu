use ../../test.nu *

# Process metadata for a well formed id that does not exist fails.

let server = spawn

let output = tg process metadata pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
assert ($output.stderr | str contains 'failed to find the process metadata') "the error should mention the missing metadata"
