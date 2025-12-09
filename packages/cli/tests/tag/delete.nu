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
failure $output "The delete command with star should fail."

# Delete a leaf tag.
let output = tg tag delete "test/1.0.0" | from json
assert (($output.deleted | length) == 1) "The command should delete one tag."

# Try to delete branch tag with children - should fail.
let output = tg tag delete "test/foo" | complete
failure $output "The command cannot delete a branch tag with children."
let stderr = $output.stderr
assert ($stderr | str contains "cannot delete branch tag") "The error message should mention branch tag."

# Delete one child leaf.
tg tag delete "test/foo/bar"

# Still cannot delete branch with remaining child.
let output = tg tag delete "test/foo" | complete
failure $output "The command still cannot delete a branch with a remaining child."

# Delete remaining child.
tg tag delete "test/foo/baz"

# Now we can delete empty branch.
let output = tg tag delete "test/foo" | from json
assert (($output.deleted | length) == 1) "The command should delete the empty branch."

# Try to delete with empty pattern - should fail.
let output = tg tag delete "" | complete
failure $output "The command cannot delete an empty pattern."
let stderr = $output.stderr
assert ($stderr | str contains "cannot delete an empty pattern") "The error message should mention empty pattern."
