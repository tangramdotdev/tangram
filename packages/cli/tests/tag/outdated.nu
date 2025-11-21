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

# Get the outdated.
let output = run tg outdated $path

snapshot -n outdated $output
