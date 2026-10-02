use ../lib/test.nu *

# Re-tagging the same node is idempotent, and forcing a new target preserves the tag ID.

let local = server spawn --config { database: { kind: sqlite, path: "database.sqlite3" } }
let database = $local.directory | path join database.sqlite3

# Create two different artifacts.
let path1 = artifact 'one'
let path2 = artifact 'two'

let id1 = tg checkin --no-tokens $path1 | referent node
let id2 = tg checkin --no-tokens $path2 | referent node

# Create the tag.
tg tag put test $id1
let initial = tg tag get test | from json
let tag_id = $initial.id
assert ('version' not-in ($initial | columns))
let initial_version = open $database | query db 'select version from tags' | get version.0
assert ($initial_version =~ '^[0123456789abcdefghjkmnpqrstvwxyz]{26}$')

# Putting the same tag and node is idempotent.
tg tag put test $id1 | complete | success $in

# A writer cannot overwrite the tag without force.
let output = tg tag put test $id2 | complete
failure $output "overwriting a tag without force should fail"
assert ($output.stderr | str contains "the tag already has a different target")
let unchanged = tg tag get test | from json
assert equal $unchanged.id $tag_id
assert equal $unchanged.target.id $id1
assert equal (open $database | query db 'select version from tags' | get version.0) $initial_version

# Force retargets the existing tag.
tg tag put --force test $id2

let tag = tg tag get test | from json
assert equal $tag.id $tag_id "The tag ID should be preserved."
assert equal $tag.target.id $id2 "The tag should point to the new node."
assert ('version' not-in ($tag | columns))
let version = open $database | query db 'select version from tags' | get version.0
assert ($version != $initial_version)
