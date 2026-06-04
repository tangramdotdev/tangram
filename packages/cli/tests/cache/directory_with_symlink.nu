use ../../test.nu *

# Caching a directory containing a symlink that targets a sibling file writes the directory into the artifacts cache.

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
let id = tg put $artifact

# Cache.
tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
