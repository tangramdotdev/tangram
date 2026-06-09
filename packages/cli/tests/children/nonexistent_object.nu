use ../../test.nu *

# Getting the children of an object by a well formed id that does not exist fails.

let server = spawn

let output = tg object children fil_010000000000000000000000000000000000000000000000000000 | complete
failure $output
assert ($output.stderr | str contains 'failed to find the object') "the error should mention the missing object"
