use ../../test.nu *

# A single tag put on a checked-in artifact can be listed and retrieved with tg list and tg tag get.

let server = spawn

# Write the artifact to a temp.
let path = artifact 'test'

# Check in.
let id = tg checkin $path

# Put tag.
let pattern = "test"
tg tag put $pattern $id

# List tags.
let list_output = tg list --no-groups
snapshot --name list $list_output

# Get tag.
let tag = tg tag get $pattern | from json
assert equal $tag.item.id $id
assert equal $tag.item.kind object
assert equal $tag.name test
assert equal $tag.specifier test
