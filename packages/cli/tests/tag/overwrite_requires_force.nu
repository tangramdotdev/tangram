use ../../test.nu *

# Re-tagging the same item is idempotent, overwriting a tag with a different item fails without --force, and --force performs the overwrite.

let server = spawn

# Create two different artifacts.
let path1 = artifact 'one'
let path2 = artifact 'two'

let id1 = tg checkin $path1
let id2 = tg checkin $path2

# Create the tag.
tg tag put test $id1

# Putting the same tag and item is idempotent.
tg tag put test $id1 | complete | success $in

# Overwriting the tag with a different item requires --force.
let output = tg tag put test $id2 | complete
failure $output "The tag command should fail without --force."
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to put the tag
	-> the request failed
	   status = 500 Internal Server Error
	-> database error
	-> the tag already exists with a different item
	-> the tag already exists with a different item

'

let item = tg tag get test | from json | get item.id
assert equal $item $id1 "The tag should still point to the original item."

# --force overwrites the tag.
tg tag put --force test $id2

let item = tg tag get test | from json | get item.id
assert equal $item $id2 "The tag should point to the new item."
