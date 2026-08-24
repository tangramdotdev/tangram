use ../../test.nu *

# A tag can be retrieved by its tag id, not just by its specifier.

let server = spawn

let path = artifact 'test'
let id = tg checkin $path
tg tag put test $id

# Capture the tag id from the specifier-based lookup.
let by_specifier = tg tag get test | from json
let tag_id = $by_specifier.id

# Get the same tag by its id.
let by_id = tg tag get $tag_id | from json
assert equal $by_id $by_specifier "getting a tag by its id should return the same record as by its specifier"
