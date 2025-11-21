use ../../test.nu *
use std assert

let remote = spawn -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

let path = artifact {
	tangram.ts: '
		export default () => "Hello, World!";

		export let metadata = {
			tag: "test-pkg/1.0.0",
		};
	'
}

let id = tg checkin $path

let tag = "test-pkg/1.0.0"
tg tag put $tag $id | complete

run tg publish $tag

# Verify tag on local.
let local_tag = tg tag get $tag | from json | get item
assert equal $local_tag $id "Local tag does not match expected ID."

# Verify tag on remote.
let remote_tag = tg --url $remote.url tag get $tag | from json | get item
assert equal $remote_tag $id "Remote tag does not match expected ID."

# Verify object synced.
let local_object = tg object get $id
let remote_object = tg --url $remote.url object get $id
assert equal $local_object $remote_object "Object not synced between local and remote."

# Index servers.
run tg --url $remote.url index
run tg index

# Verify metadata synced.
let local_metadata_result = tg object metadata $id | complete
let remote_metadata_result = tg --url $remote.url object metadata $id | complete

# Both should either succeed or fail in the same way
if $local_metadata_result.exit_code == 0 and $remote_metadata_result.exit_code == 0 {
	let local_metadata = $local_metadata_result.stdout | from json
	let remote_metadata = $remote_metadata_result.stdout | from json
	assert equal $local_metadata $remote_metadata "Metadata not synced between local and remote."
}
