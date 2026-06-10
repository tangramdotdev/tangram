use ../../test.nu *

# Getting a process by a well-formed id that does not exist fails.

let server = spawn

let output = tg get pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
assert ($output.stderr | str contains "failed to find the process") "the error should mention the missing process"
