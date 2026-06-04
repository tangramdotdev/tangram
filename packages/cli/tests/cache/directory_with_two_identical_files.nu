use ../../test.nu *

# Caching a directory containing two entries with identical contents writes the directory into the artifacts cache.

let server = spawn

# Create the artifact.
let artifact = '
	tg.directory({
		"hello.txt": "Hello, World!",
		"world.txt": "Hello, World!"
	})
'
let id = tg put $artifact

# Cache.
tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
