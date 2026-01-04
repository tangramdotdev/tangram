use ../../test.nu *

let remote = spawn --cloud -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Import a dependency in a submodule (helper.tg.ts), not in the root tangram.ts.
# Also import another internal submodule to verify it is not treated as a separate package.
let shared_path = artifact {
	dep: {
		tangram.ts: '
			export default () => "I am a dependency!";

			export let metadata = {
				tag: "test-dep/1.0.0",
			};
		'
	},
	main: {
		tangram.ts: '
			import { helper } from "./helper.tg.ts";

			export default () => `Main using: ${helper()}`;

			export let metadata = {
				tag: "test-main/1.0.0",
			};
		',
		helper.tg.ts: '
			import dep from "test-dep" with { local: "../dep" };
			import { util } from "./util.tg.ts";

			export let helper = () => `helper with ${dep()} and ${util()}`;
		',
		util.tg.ts: '
			export let util = () => "util function";
		'
	}
}

let main_path = $shared_path | path join main

# Publish the main package - this should also publish the dependency.
cd $main_path
let output = tg publish . | complete
success $output

# Verify that only 2 packages are published (test-main and test-dep), not util.tg.ts.
let publish_count = $output.stderr | lines | where {|line| $line =~ "info tagged test-" } | length
let expected_msg = $"Expected exactly 2 packages to be published, not internal submodules. Found ($publish_count) packages."
assert equal $publish_count 2 $expected_msg

# Verify that util.tg.ts is not treated as a separate package.
assert not (($output.stderr | str contains "published") and ($output.stderr | str contains "util")) "Internal submodule 'util' should not be published as a separate package."

# Extract published IDs from stderr.
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

let dep_id = do $extract_published_id "test-dep/1.0.0"
let main_id = do $extract_published_id "test-main/1.0.0"

# Verify both packages are tagged on remote.
let remote_main_tag = tg --url $remote.url tag get test-main/1.0.0 | from json | get item
let remote_dep_tag = tg --url $remote.url tag get test-dep/1.0.0 | from json | get item
assert equal $remote_main_tag $main_id "Remote main tag does not match expected ID."
assert equal $remote_dep_tag $dep_id "Remote dependency tag does not match expected ID."

# Verify objects synced.
let local_main_obj = tg object get $main_id
let remote_main_obj = tg --url $remote.url object get $main_id
let local_dep_obj = tg object get $dep_id
let remote_dep_obj = tg --url $remote.url object get $dep_id
assert equal $local_main_obj $remote_main_obj "Main object not synced between local and remote."
assert equal $local_dep_obj $remote_dep_obj "Dependency object not synced between local and remote."

# Index servers.
tg --url $remote.url index
tg index

# Verify metadata synced.
let local_main_metadata = tg object metadata $main_id | from json
let remote_main_metadata = tg --url $remote.url object metadata $main_id | from json
let local_dep_metadata = tg object metadata $dep_id | from json
let remote_dep_metadata = tg --url $remote.url object metadata $dep_id | from json
assert equal $local_main_metadata $remote_main_metadata "Main metadata not synced between local and remote."
assert equal $local_dep_metadata $remote_dep_metadata "Dependency metadata not synced between local and remote."
