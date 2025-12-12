use ../../test.nu *

let server = spawn

let path = artifact (file 'Hello, World!')

let id = tg checkin --no-cache-references $path
tg index

# Verify we can read the file contents using tg read.
let contents = tg read $id
assert equal $contents 'Hello, World!'
