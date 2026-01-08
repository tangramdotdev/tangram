use ../../test.nu *

let remote = spawn --cloud -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create a shared directory with all three packages as siblings.
let shared_path = artifact {
	transitive: {
		tangram.ts: '
			export default () => "I am the transitive dependency!";

			export let metadata = {
				tag: "test-transitive/1.0.0",
			};
		'
	},
	dep: {
		tangram.ts: '
			import transitive from "test-transitive" with { local: "../transitive" };

			export default () => `Dependency using: ${transitive()}`;

			export let metadata = {
				tag: "test-dep/1.0.0",
			};
		'
	},
	main: {
		tangram.ts: '
			import dep from "test-dep" with { local: "../dep" };

			export default () => `Main package using: ${dep()}`;

			export let metadata = {
				tag: "test-main/1.0.0",
			};
		'
	}
}

let main_path = $shared_path | path join main

# Publish the main package - this should publish C, then B, then A.
cd $main_path
let output = tg publish . | complete
success $output

# Extract the published artifact IDs from the stderr output.
let extract_published_id = {|package_name: string|
	let line = $output.stderr | lines | find --regex $"info tagged ($package_name)" | get 0?
	if $line == null {
		error make { msg: $"($package_name) should have a published ID in output." }
	}
	let id_part = $line | parse --regex 'dir_([a-z0-9]+)' | get 0.capture0?
	if $id_part == null {
		error make { msg: $"Failed to extract ID for ($package_name)." }
	}
	$"dir_($id_part)"
}

let transitive_id = do $extract_published_id "test-transitive/1.0.0"
let dep_id = do $extract_published_id "test-dep/1.0.0"
let main_id = do $extract_published_id "test-main/1.0.0"

# Verify all three packages are tagged on remote.
let remote_main_tag = tg --url $remote.url tag get test-main/1.0.0 | from json | get item
let remote_dep_tag = tg --url $remote.url tag get test-dep/1.0.0 | from json | get item
let remote_transitive_tag = tg --url $remote.url tag get test-transitive/1.0.0 | from json | get item
assert equal $remote_main_tag $main_id "Remote main tag does not match expected ID."
assert equal $remote_dep_tag $dep_id "Remote dependency tag does not match expected ID."
assert equal $remote_transitive_tag $transitive_id "Remote transitive tag does not match expected ID."

# Verify objects synced.
let local_main_obj = tg object get $main_id
let remote_main_obj = tg --url $remote.url object get $main_id
let local_dep_obj = tg object get $dep_id
let remote_dep_obj = tg --url $remote.url object get $dep_id
let local_transitive_obj = tg object get $transitive_id
let remote_transitive_obj = tg --url $remote.url object get $transitive_id
assert equal $local_main_obj $remote_main_obj "Main object not synced between local and remote."
assert equal $local_dep_obj $remote_dep_obj "Dependency object not synced between local and remote."
assert equal $local_transitive_obj $remote_transitive_obj "Transitive object not synced between local and remote."

# Index servers.
tg --url $remote.url index
tg index

# Verify metadata synced.
let local_main_metadata = tg object metadata $main_id | from json
let remote_main_metadata = tg --url $remote.url object metadata $main_id | from json
let local_dep_metadata = tg object metadata $dep_id | from json
let remote_dep_metadata = tg --url $remote.url object metadata $dep_id | from json
let local_transitive_metadata = tg object metadata $transitive_id | from json
let remote_transitive_metadata = tg --url $remote.url object metadata $transitive_id | from json
assert equal $local_main_metadata $remote_main_metadata "Main metadata not synced between local and remote."
assert equal $local_dep_metadata $remote_dep_metadata "Dependency metadata not synced between local and remote."
assert equal $local_transitive_metadata $remote_transitive_metadata "Transitive metadata not synced between local and remote."

# Verify that all published packages were re-checked in without local dependencies.
# The main package should have tag-based dependencies, not local path dependencies.
let main_object = tg get $main_id --blobs --depth=inf
assert not ($main_object | str contains '"path"') "test-main should not have local path dependencies after publishing."

# The dep package should also have tag-based dependencies.
let dep_object = tg get $dep_id --blobs --depth=inf
assert not ($dep_object | str contains '"path"') "test-dep should not have local path dependencies after publishing."

# The transitive package has no dependencies, but verify it does not have path fields.
let transitive_object = tg get $transitive_id --blobs --depth=inf
assert not ($transitive_object | str contains '"path"') "test-transitive should not have local path dependencies after publishing."
