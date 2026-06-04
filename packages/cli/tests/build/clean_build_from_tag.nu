use ../../test.nu *

# Building a tag on a second local server pulls the published artifact from the remote and produces the same output as the original build.

# Create remote and local servers.
let remote = spawn --cloud --name remote
let local1 = spawn --name local_one --config {
	remotes: { default: { url: $remote.url } }
}
let local2 = spawn --name local_two --config {
	remotes: { default: { url: $remote.url } }
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
let id = tg --url $local1.url checkin $path
let output_id = tg --url $local1.url build $id
print 'first build succeeded'

# Push the tag.
tg --url $local1.url tag test-pkg/1.0.0 $id
tg --url $local1.url push test-pkg/1.0.0

# Build from the tag. This should pull the artifact from the remote.
let output_two_id = tg --url $local2.url build test-pkg/1.0.0

# Verify the objects are the same.
assert equal $output_id $output_two_id "objects should be the same"
