use ../../test.nu *

let server = spawn

# Create and tag an artifact.
let path = artifact 'test'
let id = run tg checkin $path

# Create a deep hierarchy to test sorting by length.
let tags = ["test/1/2/3/4/5"]
for tag in $tags {
	run tg tag put $tag $id
}

# Recursively delete - should process in order from deepest to shallowest.
let output = run tg tag delete --recursive "test/*"

# Verify the order: longest paths first (children before parents).
snapshot -n deleted $output
