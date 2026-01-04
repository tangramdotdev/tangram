use ../../test.nu *

# Create remote and local servers.
let remote = spawn --cloud -n remote
let local1 = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}
let local2 = spawn -n local_two -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create a package with metadata for publishing.
let path = artifact {
	tangram.ts: '
		export default () => tg.file("Hello from published package!");

		export let metadata = {
			tag: "test-pkg/1.0.0",
		};
	'
}

# Build.
let id = tg checkin $path
let output_id = tg build $id

# Push the tag.
tg tag test-pkg/1.0.0 $id
tg push test-pkg/1.0.0

# Build from the tag. This should pull the artifact from the remote.
let output_two_id = tg -u $local2.url build test-pkg/1.0.0

# Verify the objects are the same.
assert equal $output_id $output_two_id "objects should be the same"
