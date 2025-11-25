use ../../test.nu *

let remote = spawn -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create the bottom package (D) - no dependencies.
let bottom_path = artifact {
	tangram.ts: '
		export default () => "I am the bottom of the diamond!";

		export let metadata = {
			tag: "test-bottom/1.0.0",
		};
	'
}

let bottom_id = run tg checkin $bottom_path

# Create a tag for the bottom package on the local server so it can be resolved.
tg tag put test-bottom/1.0.0 $bottom_id | complete

# Create the left package (A) - depends on bottom.
let left_path = artifact {
	tangram.ts: '
		import bottom from "test-bottom";

		export default () => `Left using: ${bottom()}`;

		export let metadata = {
			tag: "test-left/1.0.0",
		};
	'
}

let left_id = run tg checkin $left_path

# Create a tag for the left package on the local server so it can be resolved.
tg tag put test-left/1.0.0 $left_id | complete

# Create the right package (B) - depends on bottom.
let right_path = artifact {
	tangram.ts: '
		import bottom from "test-bottom";

		export default () => `Right using: ${bottom()}`;

		export let metadata = {
			tag: "test-right/1.0.0",
		};
	'
}

let right_id = run tg checkin $right_path

# Create a tag for the right package on the local server so it can be resolved.
tg tag put test-right/1.0.0 $right_id | complete

# Create the main package - depends on both left and right.
let main_path = artifact {
	tangram.ts: '
		import left from "test-left";
		import right from "test-right";

		export default () => `Main using: ${left()} and ${right()}`;

		export let metadata = {
			tag: "test-main/1.0.0",
		};
	'
}

let main_id = run tg checkin $main_path

# Publish the main package - this should publish bottom, then left and right, then main.
let output = tg publish $main_path | complete
success $output

# Verify all packages are mentioned in stderr output.
assert ($output.stderr | str contains "info tagged test-bottom/1.0.0") "test-bottom should be published."
assert ($output.stderr | str contains "info tagged test-left/1.0.0") "test-left should be published."
assert ($output.stderr | str contains "info tagged test-right/1.0.0") "test-right should be published."
assert ($output.stderr | str contains "info tagged test-main/1.0.0") "test-main should be published."

# Verify all four packages are tagged on remote.
let remote_main_tag = run tg --url $remote.url tag get test-main/1.0.0 | from json | get item
let remote_left_tag = run tg --url $remote.url tag get test-left/1.0.0 | from json | get item
let remote_right_tag = run tg --url $remote.url tag get test-right/1.0.0 | from json | get item
let remote_bottom_tag = run tg --url $remote.url tag get test-bottom/1.0.0 | from json | get item
assert equal $remote_main_tag $main_id "Remote main tag does not match expected ID."
assert equal $remote_left_tag $left_id "Remote left tag does not match expected ID."
assert equal $remote_right_tag $right_id "Remote right tag does not match expected ID."
assert equal $remote_bottom_tag $bottom_id "Remote bottom tag does not match expected ID."

# Verify objects synced.
let local_main_obj = run tg object get $main_id
let remote_main_obj = run tg --url $remote.url object get $main_id
let local_left_obj = run tg object get $left_id
let remote_left_obj = run tg --url $remote.url object get $left_id
let local_right_obj = run tg object get $right_id
let remote_right_obj = run tg --url $remote.url object get $right_id
let local_bottom_obj = run tg object get $bottom_id
let remote_bottom_obj = run tg --url $remote.url object get $bottom_id
assert equal $local_main_obj $remote_main_obj "Main object not synced between local and remote."
assert equal $local_left_obj $remote_left_obj "Left object not synced between local and remote."
assert equal $local_right_obj $remote_right_obj "Right object not synced between local and remote."
assert equal $local_bottom_obj $remote_bottom_obj "Bottom object not synced between local and remote."

# Index servers.
run tg --url $remote.url index
run tg index

# Verify metadata synced.
let local_main_metadata = run tg object metadata $main_id | from json
let remote_main_metadata = run tg --url $remote.url object metadata $main_id | from json
let local_left_metadata = run tg object metadata $left_id | from json
let remote_left_metadata = run tg --url $remote.url object metadata $left_id | from json
let local_right_metadata = run tg object metadata $right_id | from json
let remote_right_metadata = run tg --url $remote.url object metadata $right_id | from json
let local_bottom_metadata = run tg object metadata $bottom_id | from json
let remote_bottom_metadata = run tg --url $remote.url object metadata $bottom_id | from json
assert equal $local_main_metadata $remote_main_metadata "Main metadata not synced between local and remote."
assert equal $local_left_metadata $remote_left_metadata "Left metadata not synced between local and remote."
assert equal $local_right_metadata $remote_right_metadata "Right metadata not synced between local and remote."
assert equal $local_bottom_metadata $remote_bottom_metadata "Bottom metadata not synced between local and remote."
