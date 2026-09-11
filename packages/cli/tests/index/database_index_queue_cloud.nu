use ../../test.nu *

# The database index queue works with the cloud database backend.

let server = server spawn --cloud
let group = tg group create project | from json
tg index
let indexed = tg group get project | from json
assert equal $indexed.id $group.id
