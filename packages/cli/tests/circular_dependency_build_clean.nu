use ../test.nu *

# Create a server.
let server = spawn

# Package "foo" imports from "bar", and "bar" imports from "foo".
let path = artifact {
	foo: {
		tangram.ts: '
			import "../bar";
			export default () => "hello from foo";
		'
	}
	bar: {
		tangram.ts: '
			import "../foo";
			export default () => "hello from bar";
		'
	}
}

let foo_path = $path | path join foo

# Checkin the artifact.
let id = run tg -u $server.url checkin $foo_path | from json

# Tag the artifact.
run tg -u $server.url tag foo $id

# Build the artifact.
run tg -u $server.url build foo

# Clean the server.
run tg -u $server.url clean

# Build the artifact again after clean.
run tg -u $server.url build foo
