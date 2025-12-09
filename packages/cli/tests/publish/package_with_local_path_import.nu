use ../../test.nu *

let remote = spawn -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create a shared directory with both packages as siblings.
let shared_path = artifact {
	dep: {
		tangram.ts: '
			export default () => "I am a local dependency!";

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

let dep_path = $shared_path | path join dep
let main_path = $shared_path | path join main

# Checkin the dep package to get its ID, but do not create a tag.
cd $dep_path
tg checkin .

# Checkin the main package to get its ID, but do not create a tag.
cd $main_path
tg checkin .

# Publish the main package without having created tags beforehand.
# This should discover the local dep, create its tag, publish it, then publish main.
let output = tg publish . | complete
success $output

# Extract the published artifact IDs from the stderr output.
# Since packages with local path dependencies are re-checked-in, the published IDs
# may differ from the original checkin IDs.
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

let published_dep_id = do $extract_published_id "test-dep/1.0.0"
let published_main_id = do $extract_published_id "test-main/1.0.0"

# Verify both packages are tagged on local with the published IDs.
let local_dep_tag = tg tag get test-dep/1.0.0 | from json | get item
let local_main_tag = tg tag get test-main/1.0.0 | from json | get item
assert equal $local_dep_tag $published_dep_id "Local dependency tag does not match published ID."
assert equal $local_main_tag $published_main_id "Local main tag does not match published ID."

# Verify both packages are tagged on remote with the published IDs.
let remote_dep_tag = tg --url $remote.url tag get test-dep/1.0.0 | from json | get item
let remote_main_tag = tg --url $remote.url tag get test-main/1.0.0 | from json | get item
assert equal $remote_dep_tag $published_dep_id "Remote dependency tag does not match published ID."
assert equal $remote_main_tag $published_main_id "Remote main tag does not match published ID."

# Verify both packages are synced using the published IDs.
let local_dep_obj = tg object get $published_dep_id
let remote_dep_obj = tg --url $remote.url object get $published_dep_id
let local_main_obj = tg object get $published_main_id
let remote_main_obj = tg --url $remote.url object get $published_main_id
assert equal $local_dep_obj $remote_dep_obj "Dependency object not synced between local and remote."
assert equal $local_main_obj $remote_main_obj "Main object not synced between local and remote."

# Index servers.
tg --url $remote.url index
tg index

# Verify metadata synced.
let local_dep_metadata = tg object metadata $published_dep_id | from json
let remote_dep_metadata = tg --url $remote.url object metadata $published_dep_id | from json
let local_main_metadata = tg object metadata $published_main_id | from json
let remote_main_metadata = tg --url $remote.url object metadata $published_main_id | from json
assert equal $local_dep_metadata $remote_dep_metadata "Dependency metadata not synced between local and remote."
assert equal $local_main_metadata $remote_main_metadata "Main metadata not synced between local and remote."

# Verify that main package has dependency on dep by tag, not by local path.
let main_object = tg get $published_main_id --blobs --depth=inf

# The referent should have the "tag" key and NOT the "path" key.
assert ($main_object | str contains '"tag"') "Main package's dependency referent should have a 'tag' field."
assert not ($main_object | str contains '"path"') "Main package's dependency referent should not have a 'path' field."
