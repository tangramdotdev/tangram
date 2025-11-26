use ../../test.nu *

let server = spawn

# Create and tag an artifact.
let path = artifact 'test'
let id = run tg checkin $path

# Create a nested tag structure: test/a/b/c, test/a/b/d, test/a/e
let tags = ["test/a/b/c" "test/a/b/d" "test/a/e"]
for tag in $tags {
	run tg tag put $tag $id
}

# Verify tags exist.
let output = run tg tag list "test/*"
assert (($output | from json | length) > 0) "the tags should exist"

# Recursively delete from the root - should delete all children in correct order.
let output = run tg tag delete --recursive "test/*"
snapshot -n deleted $output

# Verify all tags are deleted.
let output = run tg tag list "test/*"
snapshot -n list $output
