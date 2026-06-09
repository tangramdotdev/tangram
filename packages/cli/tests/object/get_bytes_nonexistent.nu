use ../../test.nu *

# Getting the raw bytes of an object that does not exist fails with a missing-object error.

let server = spawn

let output = tg object get fil_010000000000000000000000000000000000000000000000000000 --bytes | complete
failure $output
assert ($output.stderr | str contains "failed to find the object") "the error should mention the missing object"
