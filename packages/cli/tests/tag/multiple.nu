use ../../test.nu *

let server = spawn

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
	tg tag put $tag $id
}

# List empty pattern.
let output = tg list --no-groups ""
snapshot -n "list_empty" $output

# List test.
let output = tg list --no-groups "test"
snapshot -n "list_test" $output

# Operators are not allowed in parent components.
let output = tg list --no-groups "test/*/*" | complete
failure $output "The command should reject operators in parent components."
assert ($output.stderr | str contains "invalid parent") "The error should mention the parent."

# List test/*
let output = tg list --no-groups "test/*"
snapshot -n "list_test_star" $output

# Operators are not allowed in parent components.
let output = tg list --no-groups "test/=0.0.1/*" | complete
failure $output "The command should reject operators in parent components."
assert ($output.stderr | str contains "invalid parent") "The error should mention the parent."

# List test/=0.0.1
let output = tg list --no-groups "test/=0.0.1"
snapshot -n "list_test_exact" $output

# List test/* recursive.
let output = tg list --no-groups --recursive "test/*"
snapshot -n "list_test_star_recursive" $output

# List test recursive.
let output = tg list --no-groups --recursive "test"
snapshot -n "list_test_recursive" $output

# Get test/1.2.0 (exact tag).
let output = tg tag get "test/1.2.0"
snapshot -n "get_test_exact" $output

# List test/^1 (latest matching ^1).
let output = tg list --no-groups --reverse "test/^1"
snapshot -n "list_test_caret1" $output

# List test/^10 (latest matching ^10).
let output = tg list --no-groups --reverse "test/^10"
snapshot -n "list_test_caret10" $output
