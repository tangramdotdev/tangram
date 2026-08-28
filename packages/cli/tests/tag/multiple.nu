use ../../test.nu *

# tg match and tg tag get over many tags handle empty, prefix, exact, and version-range patterns correctly and reject pattern operators inside parent components.

let server = server spawn

# Write the artifact to a temp.
let path = artifact 'Hello, World!'

# Check in.
let id = tg checkin $path

# Tag the objects.
let tags = [
	"foo"
	"bar"
	"test/0.0.1/foo/bar"
	"test/1.0.0"
	"test/1.1.0"
	"test/1.2.0"
	"test/10.0.0"
	"test/foo/bar/baz"
	"test/hello/1.0.0"
	"test/world/1.0.0"
]

for tag in $tags {
	tg tag put -p $tag $id
}

# Empty pattern is not valid.
let output = tg match --no-groups "" | complete
failure $output "The command should reject an empty pattern."
snapshot --normalize $output.stderr r#'
	error: invalid value '' for '<PATTERN>': invalid specifier pattern
	
	For more information, try '--help'.

'#

# List the direct children of test.
let output = tg list --no-groups "test"
snapshot --normalize --name "list_test" $output

# Operators are not allowed in parent components.
let output = tg match --no-groups "test/*/*" | complete
failure $output "The command should reject operators in parent components."
snapshot --normalize $output.stderr r#'
	error: invalid value 'test/*/*' for '<PATTERN>': invalid parent
	
	For more information, try '--help'.

'#

# List test/*
let output = tg match --no-groups "test/*"
snapshot --normalize --name "list_test_star" $output

# Operators are not allowed in parent components.
let output = tg match --no-groups "test/=0.0.1/*" | complete
failure $output "The command should reject operators in parent components."
snapshot --normalize $output.stderr r#'
	error: invalid value 'test/=0.0.1/*' for '<PATTERN>': invalid parent
	
	For more information, try '--help'.

'#

# List test/=0.0.1
let output = tg match --no-groups "test/=0.0.1"
snapshot --normalize --name "list_test_exact" $output

# List all tag descendants.
let output = tg list --no-groups --recursive test
snapshot --normalize --name "list_test_star_recursive" $output

# List all tag descendants.
let output = tg list --no-groups --recursive test
snapshot --normalize --name "list_test_recursive" $output

# Get test/1.2.0 (exact tag).
let tag = tg tag get "test/1.2.0" | from json
assert equal $tag.target.id $id
assert equal $tag.target.kind object
assert equal $tag.name "1.2.0"
assert equal $tag.specifier "test/1.2.0"

# List test/^1 (latest matching ^1).
let output = tg match --no-groups --reverse "test/^1"
snapshot --normalize --name "list_test_caret1" $output

# List test/^10 (latest matching ^10).
let output = tg match --no-groups --reverse "test/^10"
snapshot --normalize --name "list_test_caret10" $output
