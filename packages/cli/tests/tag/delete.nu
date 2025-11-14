use std assert
use ../../test.nu *

let server = spawn

# Create and tag an artifact.
let path = artifact 'test'
let id = tg checkin $path

# Create a mix of leaf tags and nested structure.
let tags = ["test/1.0.0" "test/2.0.0" "test/foo/bar" "test/foo/baz"]
for tag in $tags {
	tg tag put $tag $id
}

# Delete with star should fail.
let output = tg tag delete "test/*" | complete
assert not equal $output.exit_code 0 'delete with star should fail'

# Delete a leaf tag.
let output = tg tag delete "test/1.0.0" | from json
assert (($output.deleted | length) == 1) 'should delete one tag'

# Try to delete branch tag with children - should fail.
let output = tg tag delete "test/foo" | complete
assert not equal $output.exit_code 0 'cannot delete branch tag with children'
let stderr = $output.stderr
assert ($stderr | str contains "cannot delete branch tag") 'error message should mention branch tag'

# Delete one child leaf.
tg tag delete "test/foo/bar"

# Still cannot delete branch with remaining child.
let output = tg tag delete "test/foo" | complete
assert not equal $output.exit_code 0 'still cannot delete branch with remaining child'

# Delete remaining child.
tg tag delete "test/foo/baz"

# Now we can delete empty branch.
let output = tg tag delete "test/foo" | from json
assert (($output.deleted | length) == 1) 'should delete empty branch'

# Try to delete with empty pattern - should fail.
let output = tg tag delete "" | complete
assert not equal $output.exit_code 0 'cannot delete empty pattern'
let stderr = $output.stderr
assert ($stderr | str contains "cannot delete an empty pattern") 'error message should mention empty pattern'
