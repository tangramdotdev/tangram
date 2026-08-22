use ../../test.nu *

# Pushing by a tag specifier transfers the object and creates the tag on the remote.

let remote = spawn --cloud --name remote
let local = spawn --name local
tg remote put default $remote.url | complete

let path = artifact {
	nested: {
		'hello.txt': 'hello'
	}
}
let id = tg checkin $path
tg tag put -p test/1.0.0 $id

# Push the group and its children.
tg push --group-children test

# The complete object subtree is present on the remote.
let local_object = tg get $id --blobs --depth=inf --pretty
let remote_object = tg --url $remote.url get $id --blobs --depth=inf --pretty
assert equal $local_object $remote_object

# The tag is present on the remote and points to the same node.
let remote_tag = tg --url $remote.url tag get test/1.0.0 | from json
assert equal $remote_tag.target.id $id "the remote tag should point to the pushed object"
assert equal $remote_tag.specifier "test/1.0.0" "the remote tag should keep its specifier"
let local_tag = tg tag get test/1.0.0 | from json
assert equal $remote_tag.id $local_tag.id "the remote tag should keep its id"

# The group also retains its ID.
let local_group = tg group get test | from json
let remote_group = tg --url $remote.url group get test | from json
assert equal $remote_group.id $local_group.id "the remote group should keep its id"

# A later push requires force to overwrite a remote change.
let local_id = tg put 'tg.file("local update")' | str trim
tg tag put --force test/1.0.0 $local_id
let remote_id = tg --url $remote.url put 'tg.file("remote update")' | str trim
tg --url $remote.url tag put --force test/1.0.0 $remote_id
failure (tg --url $remote.url object get $local_id | complete)
let output = tg push test/1.0.0 | complete
failure $output "a push should not retarget an existing tag without force"
assert ($output.stderr | str contains "the tag already has a different target")
let remote_tag = tg --url $remote.url tag get test/1.0.0 | from json
assert equal $remote_tag.target.id $remote_id "the failed push should preserve the remote tag"
tg push --force test/1.0.0
let remote_tag = tg --url $remote.url tag get test/1.0.0 | from json
assert equal $remote_tag.target.id $local_id "the local tag should overwrite the remote tag"
success (tg --url $remote.url object get $local_id | complete)
