use ../../test.nu *
use std assert

let remote = spawn -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create and publish the dependency package.
let dep_path = artifact {
	tangram.ts: '
		export default () => "I am a dependency!";

		export let metadata = {
			tag: "test-dep/1.0.0",
		};
	'
}

let dep_id = tg checkin $dep_path
tg publish $dep_path | complete | success $in

# Create a package that depends on the first package.
let main_path = artifact {
	tangram.ts: '
		import dep from "test-dep";

		export default () => `Main package using: ${dep()}`;

		export let metadata = {
			tag: "test-main/1.0.0",
		};
	'
}

let main_id = tg checkin $main_path
tg publish $main_path | complete | success $in

# Verify tags on local.
let local_main_tag = tg tag get test-main/1.0.0 | from json | get item
let local_dep_tag = tg tag get test-dep/1.0.0 | from json | get item
assert equal $local_main_tag $main_id "Local main tag does not match expected ID."
assert equal $local_dep_tag $dep_id "Local dependency tag does not match expected ID."

# Verify tags on remote.
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
run tg --url $remote.url index
run tg index

# Verify metadata synced.
let local_main_metadata = tg object metadata $main_id | from json
let remote_main_metadata = tg --url $remote.url object metadata $main_id | from json
let local_dep_metadata = tg object metadata $dep_id | from json
let remote_dep_metadata = tg --url $remote.url object metadata $dep_id | from json
assert equal $local_main_metadata $remote_main_metadata "Main metadata not synced between local and remote."
assert equal $local_dep_metadata $remote_dep_metadata "Dependency metadata not synced between local and remote."
