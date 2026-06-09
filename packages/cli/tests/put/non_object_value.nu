use ../../test.nu *

# Putting a value that is not an object fails because only objects have ids to print.

let server = spawn

let output = tg put '42' | complete
failure $output
assert ($output.stderr | str contains "expected an object value") "the error should mention the object requirement"
