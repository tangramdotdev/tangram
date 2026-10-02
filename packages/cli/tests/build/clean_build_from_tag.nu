use ../lib/test.nu *

# Building a tag on a second local server pulls the published artifact from the remote and produces the same output as the original build.

# Create remote and local servers.
let remote = server spawn --cloud --name remote
let local_one = server spawn --name local-one --config {
	remotes: { default: { url: $remote.url } }
}
let local_two = server spawn --name local-two --config {
	remotes: { default: { url: $remote.url } }
}

# Create a package with metadata for publishing.
let path = artifact {
	tangram.ts: '
		export default function () { return tg.file("Hello from published package!"); }

		export let metadata = {
			tag: "test-pkg/1.0.0",
		};
	'
}

# Build.
let id = tg --url $local_one.url checkin --no-tokens $path | referent node
let output_id = tg --url $local_one.url build --no-tokens $id
print 'first build succeeded'

# Push the tag.
tg --url $local_one.url tag -p test-pkg/1.0.0 $id
tg --url $local_one.url push --group-children test-pkg

# Build from the tag. This should pull the artifact from the remote.
let output_two_id = tg --url $local_two.url build --no-tokens test-pkg/1.0.0

# Verify the objects are the same.
let output_id = $output_id | str trim
let output_two_id = $output_two_id | str trim
assert equal $output_id $output_two_id "objects should be the same"
