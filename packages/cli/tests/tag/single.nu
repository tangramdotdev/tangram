use ../../test.nu *

# A single tag put on a checked-in artifact can be listed and retrieved with tg list and tg tag get.

let server = server spawn

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
assert (($list.0 | get --optional node.options.tokens.local) != null) "list should return a token"
snapshot --normalize --name list $list_output

# Get the tag.
let output = with-env { TANGRAM_QUIET: "false" } { tg tag get $pattern | complete }
success $output
let tag = $output.stdout | from json
assert equal $tag.target.id $id
assert equal $tag.target.kind object
assert equal $tag.name test
assert equal $tag.specifier test
assert (($tag | get --optional location) == null) "tag get should not print a location to stdout"
assert (($tag | get --optional tokens) == null) "tag get should not print tokens to stdout"
assert equal ($output.stderr | lines | length) 2 "tag get should print location and token info"
