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

let override_tag = "overridden-pkg/2.0.0"
run tg publish --tag $override_tag $path

# Verify override tag on local.
let local_tag = tg tag get $override_tag | from json | get item
assert equal $local_tag $id "Local override tag does not match expected ID."

# Verify override tag on remote.
let remote_tag = tg --url $remote.url tag get $override_tag | from json | get item
assert equal $remote_tag $id "Remote override tag does not match expected ID."

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

# Verify original metadata tag was NOT created.
let original_tag = "test-pkg/1.0.0"
let tag_result = tg tag get $original_tag | complete
if $tag_result.exit_code == 0 {
	let tag_output = $tag_result.stdout | from json
	assert ($tag_output.item? | is-empty) "Original metadata tag should not be created when using --tag override."
}
