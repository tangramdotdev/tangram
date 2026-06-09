use ../../test.nu *

# Putting bytes under an id they do not hash to is rejected by the server, and the object is not stored.

let server = spawn

let real = tg put 'tg.file("real")' | str trim
let bytes = tg get $real --bytes
let fake = "fil_010000000000000000000000000000000000000000000000000000"

let output = $bytes | tg object put $fake --bytes | complete
failure $output
assert ($output.stderr | str contains "invalid object id") "the error should mention the id mismatch"

let output = tg get $fake | complete
failure $output
