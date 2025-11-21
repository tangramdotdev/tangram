use ../../test.nu *

let server = spawn

# Write the artifact to a temp.
let path = artifact 'Hello, World!'

# Check in.
let id = run tg checkin $path

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
	run tg tag put $tag $id
}

# List empty pattern.
let output = run tg tag list ""
snapshot -n "list_empty" $output

# List test.
let output = run tg tag list "test"
snapshot -n "list_test" $output

# List test/*/*
let output = run tg tag list "test/*/*"
snapshot -n "list_test_star_star" $output

# List test/*
let output = run tg tag list "test/*"
snapshot -n "list_test_star" $output

# List test/=0.0.1/*
let output = run tg tag list "test/=0.0.1/*"
snapshot -n "list_test_exact_star" $output

# List test/=0.0.1
let output = run tg tag list "test/=0.0.1"
snapshot -n "list_test_exact" $output

# List test/* recursive.
let output = run tg tag list --recursive "test/*"
snapshot -n "list_test_star_recursive" $output

# List test recursive.
let output = run tg tag list --recursive "test"
snapshot -n "list_test_recursive" $output

# Get test.
let output = run tg tag get "test"
snapshot -n "get_test" $output

# Get test/^1
let output = run tg tag get "test/^1"
snapshot -n "get_test_caret1" $output

# Get test/^10
let output = run tg tag get "test/^10"
snapshot -n "get_test_caret10" $output
