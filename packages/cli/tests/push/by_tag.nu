use ../../test.nu *

# Pushing by a tag specifier transfers the object and creates the tag on the remote.

let remote = spawn --cloud --name remote
let local = spawn --name local
tg remote put default $remote.url | complete

let path = artifact 'hello'
let id = tg checkin $path
tg tag put test/1.0.0 $id

# Push by the tag specifier.
tg push test/1.0.0

# The object is present on the remote.
let object = tg --url $remote.url object get $id | complete
success $object

# The tag is present on the remote and points to the same item.
let remote_tag = tg --url $remote.url tag get test/1.0.0 | from json
assert equal $remote_tag.item.id $id "the remote tag should point to the pushed object"
assert equal $remote_tag.specifier "test/1.0.0" "the remote tag should keep its specifier"
