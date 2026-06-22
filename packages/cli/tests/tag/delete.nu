use ../../test.nu *

# tg tag delete removes a single leaf tag, refuses star patterns and empty patterns, and does not delete a namespace that still has or once had children.

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

# Deleting a parent as a tag should not delete its children.
let output = tg tag delete "test/foo" | from json
assert (($output.deleted | length) == 0) "The command should not delete a parent."

# Delete one child leaf.
tg tag delete "test/foo/bar"

# Deleting the parent should still not delete the remaining child.
let output = tg tag delete "test/foo" | from json
assert (($output.deleted | length) == 0) "The command should not delete a parent."

# Delete remaining child.
tg tag delete "test/foo/baz"

# Deleting the empty parent as a tag should still delete nothing.
let output = tg tag delete "test/foo" | from json
assert (($output.deleted | length) == 0) "The command should not delete a parent."

# Try to delete with empty pattern - should fail.
let output = tg tag delete "" | complete
failure $output "The command should reject an empty pattern."
snapshot ($output.stderr | redact) r#'
	error: invalid value '' for '<PATTERN>': invalid specifier pattern
	
	For more information, try '--help'.

'#
