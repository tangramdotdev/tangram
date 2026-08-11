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
let list = $list_output | from json
assert (($list.0 | get --optional token) != null) "list should return a token"
snapshot --normalize --name list $list_output

# Get tag.
let tag = tg tag get $pattern | from json
assert equal $tag.target.id $id
assert equal $tag.target.kind object
assert equal $tag.name test
assert equal $tag.specifier test
assert (($tag | get --optional token) != null) "tag get should return a token"
