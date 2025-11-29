use ../../test.nu *

# Create remote and local servers.
let remote = spawn -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}
let local_two = spawn -n local_two -c {
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
let id = run tg checkin $path
let output_id = run tg build $id

# Push the tag.
run tg tag test-pkg/1.0.0 $id
run tg push test-pkg/1.0.0

# Build from the tag. This should pull the artifact from the remote.
let output_two_id = run tg -u $local_two.url build test-pkg/1.0.0

# Verify the objects are the same.
assert equal $output_id $output_two_id "objects should be the same"
