# Publishing a package with the same metadata tag requires force to replace the tag.
use ../../test.nu *

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}
# Create and publish the first package.
let path1 = artifact {
	tangram.ts: '
		export default function () { return "Hello, World!"; }

		export let metadata = {
			tag: "test-pkg/1.0.0",
		};
	'
}

let id1 = tg checkin $path1
tg publish $path1

# Verify the tag points to the first package.
let local_tag1 = tg tag get test-pkg/1.0.0 | from json | get target.id
assert equal $local_tag1 $id1 "Local tag should point to first package."

let remote_tag1 = tg --url $remote.url tag get test-pkg/1.0.0 | from json | get target.id
assert equal $remote_tag1 $id1 "Remote tag should point to first package."

# Create and publish a second package with the same metadata tag but different content.
let path2 = artifact {
	tangram.ts: '
		export default function () { return "Goodbye, World!"; }

		export let metadata = {
			tag: "test-pkg/1.0.0",
		};
	'
}

let id2 = tg checkin $path2

# The two packages should have different IDs.
assert not equal $id1 $id2 "The two packages should have different IDs."

# Republish without force.
let output = tg publish $path2 | complete
failure $output "republishing without force should fail"

# Verify both tags still point to the first package.
let local_tag2 = tg tag get test-pkg/1.0.0 | from json | get target.id
assert equal $local_tag2 $id1 "The local tag should not be replaced without force."
let remote_tag2 = tg --url $remote.url tag get test-pkg/1.0.0 | from json | get target.id
assert equal $remote_tag2 $id1 "The remote tag should not be replaced without force."

# Republish with force.
tg publish --force $path2

# Verify the tag now points to the second package on local.
let local_tag3 = tg tag get test-pkg/1.0.0 | from json | get target.id
assert equal $local_tag3 $id2 "Local tag should now point to second package after republish."

# Verify the tag now points to the second package on remote.
let remote_tag3 = tg --url $remote.url tag get test-pkg/1.0.0 | from json | get target.id
assert equal $remote_tag3 $id2 "Remote tag should now point to second package after republish."

# Verify the second object is synced.
let local_object = tg object get $id2
let remote_object = tg --url $remote.url object get $id2
assert equal $local_object $remote_object "Second object not synced between local and remote."

# Index servers.
tg --url $remote.url index
tg index

# Verify metadata synced for the second package.
let local_metadata = tg object metadata $id2 | from json
let remote_metadata = tg --url $remote.url object metadata $id2 | from json
assert equal $local_metadata $remote_metadata "Metadata not synced between local and remote."
