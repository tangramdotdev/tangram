use ../../test.nu *

let remote = spawn -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create a single-file package.
let single_file_content = '
	export default () => "I am a single-file package!";

	export let metadata = {
		tag: "test-single-file/1.0.0",
	};
'

let single_file_dir = mktemp -d
let single_file_path = $single_file_dir | path join "package.ts"
$single_file_content | save $single_file_path

let single_file_id = tg checkin $single_file_path
tg tag put test-single-file/1.0.0 $single_file_id | complete

# Create a multi-file package with submodules.
let multi_file_path = artifact {
	tangram.ts: '
		import { helper } from "./helper.ts";
		import { util } from "./subdir/util.ts";

		export default () => `Multi-file using: ${helper()} and ${util()}`;

		export let metadata = {
			tag: "test-multi-file/1.0.0",
		};
	',
	helper.ts: '
		export let helper = () => "helper function";
	',
	subdir: {
		util.ts: '
			export let util = () => "util function";
		'
	}
}

let multi_file_id = tg checkin $multi_file_path
tg tag put test-multi-file/1.0.0 $multi_file_id | complete

# Create a main package that imports both.
let main_path = artifact {
	tangram.ts: '
		import singleFile from "test-single-file";
		import multiFile from "test-multi-file";

		export default () => `Main using: ${singleFile()} and ${multiFile()}`;

		export let metadata = {
			tag: "test-main/1.0.0",
		};
	'
}

let main_id = tg checkin $main_path

# Publish and capture the output to check what gets published.
let output = tg publish $main_path | complete
success $output

# Count how many packages are being published.
let publish_count = $output.stderr | lines | where {|line| $line =~ "info tagged test-" } | length

# We should only publish 3 packages: test-main, test-single-file, test-multi-file.
# We should NOT publish helper.ts or util.ts as separate packages.
let expected_msg = $"Expected 3 packages to be published but found ($publish_count)."
assert equal $publish_count 3 $expected_msg

# Verify the correct packages are published.
assert ($output.stderr | str contains "info tagged test-main/1.0.0") "test-main should be published."
assert ($output.stderr | str contains "info tagged test-single-file/1.0.0") "test-single-file should be published."
assert ($output.stderr | str contains "info tagged test-multi-file/1.0.0") "test-multi-file should be published."

# Verify all packages are tagged on local.
let local_main_tag = tg tag get test-main/1.0.0 | from json | get item
let local_single_tag = tg tag get test-single-file/1.0.0 | from json | get item
let local_multi_tag = tg tag get test-multi-file/1.0.0 | from json | get item
assert equal $local_main_tag $main_id "Local main tag does not match expected ID."
assert equal $local_single_tag $single_file_id "Local single-file tag does not match expected ID."
assert equal $local_multi_tag $multi_file_id "Local multi-file tag does not match expected ID."

# Verify all packages are tagged on remote.
let remote_main_tag = tg --url $remote.url tag get test-main/1.0.0 | from json | get item
let remote_single_tag = tg --url $remote.url tag get test-single-file/1.0.0 | from json | get item
let remote_multi_tag = tg --url $remote.url tag get test-multi-file/1.0.0 | from json | get item
assert equal $remote_main_tag $main_id "Remote main tag does not match expected ID."
assert equal $remote_single_tag $single_file_id "Remote single-file tag does not match expected ID."
assert equal $remote_multi_tag $multi_file_id "Remote multi-file tag does not match expected ID."

# Verify objects synced.
let local_main_obj = tg object get $main_id
let remote_main_obj = tg --url $remote.url object get $main_id
let local_single_obj = tg object get $single_file_id
let remote_single_obj = tg --url $remote.url object get $single_file_id
let local_multi_obj = tg object get $multi_file_id
let remote_multi_obj = tg --url $remote.url object get $multi_file_id
assert equal $local_main_obj $remote_main_obj "Main object not synced between local and remote."
assert equal $local_single_obj $remote_single_obj "Single-file object not synced between local and remote."
assert equal $local_multi_obj $remote_multi_obj "Multi-file object not synced between local and remote."

# Index servers.
tg --url $remote.url index
tg index

# Verify metadata synced.
let local_main_metadata = tg object metadata $main_id | from json
let remote_main_metadata = tg --url $remote.url object metadata $main_id | from json
let local_single_metadata = tg object metadata $single_file_id | from json
let remote_single_metadata = tg --url $remote.url object metadata $single_file_id | from json
let local_multi_metadata = tg object metadata $multi_file_id | from json
let remote_multi_metadata = tg --url $remote.url object metadata $multi_file_id | from json
assert equal $local_main_metadata $remote_main_metadata "Main metadata not synced between local and remote."
assert equal $local_single_metadata $remote_single_metadata "Single-file metadata not synced between local and remote."
assert equal $local_multi_metadata $remote_multi_metadata "Multi-file metadata not synced between local and remote."
