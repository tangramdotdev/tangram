use ../../test.nu *

let server = spawn

# Create the artifact.
let artifact = '
	tg.directory({
		"hello.txt": "Hello, World!"
	})
'
let id = run tg put $artifact

# Cache.
let output = run tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
