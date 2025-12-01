use ../../test.nu *

let server = spawn

# Write the artifact to a temp.
let path = artifact 'Hello, World!'

# Check in.
let id = run tg checkin $path

# Tag it a couple times.
for version in ["1.0.0" "1.1.0" "2.0.0"] {
	run tg tag put $"hello/($version)" $id
}

# Create something that uses it.
let path = artifact {
	tangram.ts: '
		import hello from "hello/^1.0";
	'
}

let output = run tg outdated --pretty $path
snapshot $output '
	[
	  {
	    "compatible": "hello/1.1.0",
	    "current": "hello/1.1.0",
	    "latest": "hello/2.0.0",
	  },
	]
'
