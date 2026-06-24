use ../../test.nu *

# Publishing a package that imports a sibling dependency by source path discovers and publishes the dependency, tags both packages on the local and remote servers, syncs objects and metadata, and rewrites the dependency reference to use a tag rather than a path.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Create a shared directory with both packages as siblings.
let shared_path = artifact {
	dep: {
		tangram.ts: '
			export default function () { return "I am a source dependency!"; }

			export let metadata = {
				tag: "test-dep/1.0.0",
			};
		'
	},
	main: {
		tangram.ts: '
			import dep from "test-dep" with { source: "../dep" };

			export default function () { return `Main package using: ${dep()}`; }

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
# This should discover the source dep, create its tag, publish it, then publish main.
let output = tg --no-quiet publish . | complete
success $output

# Extract the published artifact IDs from the stderr output.
# Since packages with source dependencies are re-checked-in, the published IDs
# may differ from the original checkin IDs.
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

let published_dep_id = do $extract_published_id "test-dep/1.0.0"
let published_main_id = do $extract_published_id "test-main/1.0.0"

# Verify both packages are tagged on local with the published IDs.
let local_dep_tag = tg tag get test-dep/1.0.0 | from json | get item.id
let local_main_tag = tg tag get test-main/1.0.0 | from json | get item.id
assert equal $local_dep_tag $published_dep_id "Local dependency tag does not match published ID."
assert equal $local_main_tag $published_main_id "Local main tag does not match published ID."

# Verify both packages are tagged on remote with the published IDs.
let remote_dep_tag = tg --url $remote.url tag get test-dep/1.0.0 | from json | get item.id
let remote_main_tag = tg --url $remote.url tag get test-main/1.0.0 | from json | get item.id
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

# The referent should use a tag, not a path.
snapshot ($main_object | normalize_ids) 'tg.directory({"tangram.ts":tg.file({"contents":tg.blob("import dep from \"test-dep\" with { source: \"../dep\" };\n\nexport default function () { return `Main package using: ${dep()}`; }\n\nexport let metadata = {\n\ttag: \"test-main/1.0.0\",\n};"),"dependencies":{"test-dep?source=../dep":{"item":tg.directory({"tangram.ts":tg.file({"contents":tg.blob("export default function () { return \"I am a source dependency!\"; }\n\nexport let metadata = {\n\ttag: \"test-dep/1.0.0\",\n};"),"module":"ts"})}),"options":{"id":"dir_010000000000000000000000000000000000000000000000000000","tag":"test-dep/1.0.0"}}},"module":"ts"})})'
