use ../../test.nu *

# A failed builtin build reports its error on stderr instead of exiting silently.

let server = spawn

let blob = "hello, world!\n" | tg write

# Decompressing a blob that is not compressed fails inside the builtin.
let output = tg decompress $blob | complete
failure $output
assert ($output.stderr | str contains "invalid compression format") "the error should be reported on stderr"
