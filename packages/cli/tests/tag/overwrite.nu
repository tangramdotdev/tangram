use ../../test.nu *

# Re-tagging the same node is idempotent, and forcing a new target preserves the tag ID.

let server = server spawn

# Create two different artifacts.
let path1 = artifact 'one'
let path2 = artifact 'two'

let id1 = tg checkin $path1
let id2 = tg checkin $path2

# Create the tag.
tg tag put test $id1
let tag_id = tg tag get test | from json | get id

# Putting the same tag and node is idempotent.
tg tag put test $id1 | complete | success $in

# A writer cannot overwrite the tag without force.
let output = tg tag put test $id2 | complete
failure $output "overwriting a tag without force should fail"
assert ($output.stderr | str contains "the tag already has a different target")
let unchanged = tg tag get test | from json
assert equal $unchanged.id $tag_id
assert equal $unchanged.target.id $id1

# Force retargets the existing tag.
tg tag put --force test $id2

let tag = tg tag get test | from json
assert equal $tag.id $tag_id "The tag ID should be preserved."
assert equal $tag.target.id $id2 "The tag should point to the new node."
