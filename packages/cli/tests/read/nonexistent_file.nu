use ../../test.nu *

# Reading a well formed file id that does not exist fails.

let server = spawn

let output = tg read fil_010000000000000000000000000000000000000000000000000000 | complete
failure $output
assert ($output.stderr | str contains "failed to load the object") "the error should mention the failed load"
