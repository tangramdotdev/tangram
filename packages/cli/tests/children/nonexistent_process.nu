use ../../test.nu *

# Getting the children of a process by a well formed id that does not exist fails.

let server = spawn

let output = tg process children pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
assert ($output.stderr | str contains 'failed to get the process') "the error should mention the missing process"
