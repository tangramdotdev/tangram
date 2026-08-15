use ../../test.nu *

# A file checked in with --no-checkout-pointers can still be read back by its object ID.

let server = spawn

let path = artifact 'Hello, World!'

let id = tg checkin --no-checkout-pointers $path
tg index

# Verify we can read the file contents using tg read.
let contents = tg read $id
assert equal $contents 'Hello, World!'
