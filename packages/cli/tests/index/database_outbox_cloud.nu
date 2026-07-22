use ../../test.nu *

# The database outbox works with the cloud database backend.

let server = spawn --cloud
let group = tg group create project | from json
tg index
let indexed = tg group get project | from json
assert equal $indexed.id $group.id
