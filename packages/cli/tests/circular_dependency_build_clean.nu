use ../test.nu *

# A package with a circular dependency between two packages can be built, cleaned, and built again successfully.

# Create a server.
let server = spawn

# Package "foo" imports from "bar", and "bar" imports from "foo".
let path = artifact {
	foo: {
		tangram.ts: '
			import "../bar";
			export default function () { return "hello from foo"; }
		'
	}
	bar: {
		tangram.ts: '
			import "../foo";
			export default function () { return "hello from bar"; }
		'
	}
}

let foo_path = $path | path join foo

# Checkin the artifact.
let id = tg --url $server.url checkin $foo_path | from json

# Tag the artifact.
tg --url $server.url tag foo $id

# Build the artifact.
tg --url $server.url build foo

# Clean the server.
tg --url $server.url clean

# Build the artifact again after clean.
tg --url $server.url build foo
