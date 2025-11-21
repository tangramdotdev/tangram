use ../../test.nu *

let server = spawn

# Create the artifact.
let artifact = '
	tg.directory({
		"hello.txt": "Hello, World!",
		"link": tg.symlink({
			"path": "hello.txt"
		})
	})
'
let id = run tg put $artifact

# Cache.
run tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
