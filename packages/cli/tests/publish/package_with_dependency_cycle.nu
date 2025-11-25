use ../../test.nu *

let remote = spawn -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create an import cycle but NOT a process cycle.
let shared_path = artifact {
	a: {
		tangram.ts: '
			import b from "test-b" with { local: "../b" };
			export default () => `A using: ${b()}`;
			export let greeting = () => "Hello from A";
			export let metadata = {
				tag: "test-a/1.0.0",
			};
		'
	},
	b: {
		tangram.ts: '
			import * as a from "test-a" with { local: "../a" };
			export default () => `B using: ${a.greeting()}`;
			export let metadata = {
				tag: "test-b/1.0.0",
			};
		'
	}
}

let b_path = $shared_path | path join b
cd $b_path
let output = tg publish . | complete
success $output

# Extract the published IDs from the stderr.
let extract_published_id = {|package_name: string|
	let line = $output.stderr | lines | find --regex $"info tagged ($package_name)" | get 0?
	if $line == null {
		error make { msg: $"($package_name) should have a published ID in output." }
	}
	let id_part = $line | parse --regex 'dir_([a-z0-9]+)' | get capture0.0?
	if $id_part == null {
		error make { msg: $"Failed to extract ID for ($package_name)." }
	}
	$"dir_($id_part)"
}

let a_published_id = do $extract_published_id "test-a/1.0.0"
let b_published_id = do $extract_published_id "test-b/1.0.0"

# Verify both packages are tagged on local.
let local_a_tag = run tg tag get test-a/1.0.0 | from json | get item
let local_b_tag = run tg tag get test-b/1.0.0 | from json | get item
assert equal $local_a_tag $a_published_id "Local tag for test-a does not match published ID."
assert equal $local_b_tag $b_published_id "Local tag for test-b does not match published ID."

# Verify both packages are tagged on remote.
let remote_a_tag = run tg --url $remote.url tag get test-a/1.0.0 | from json | get item
let remote_b_tag = run tg --url $remote.url tag get test-b/1.0.0 | from json | get item
assert equal $remote_a_tag $a_published_id "Remote tag for test-a does not match published ID."
assert equal $remote_b_tag $b_published_id "Remote tag for test-b does not match published ID."

# Verify objects are synced to remote.
let local_a_obj = run tg object get $a_published_id
let remote_a_obj = run tg --url $remote.url object get $a_published_id
let local_b_obj = run tg object get $b_published_id
let remote_b_obj = run tg --url $remote.url object get $b_published_id
assert equal $local_a_obj $remote_a_obj "Package A object not synced between local and remote."
assert equal $local_b_obj $remote_b_obj "Package B object not synced between local and remote."

# Index servers.
run tg --url $remote.url index
run tg index

# Verify metadata synced.
let local_a_metadata = run tg object metadata $a_published_id | from json
let remote_a_metadata = run tg --url $remote.url object metadata $a_published_id | from json
let local_b_metadata = run tg object metadata $b_published_id | from json
let remote_b_metadata = run tg --url $remote.url object metadata $b_published_id | from json
assert equal $local_a_metadata $remote_a_metadata "Package A metadata not synced between local and remote."
assert equal $local_b_metadata $remote_b_metadata "Package B metadata not synced between local and remote."
