use ../../test.nu *

# Caching a simple file writes the file into the artifacts cache.

let server = spawn

# Create the artifact.
let artifact = '
	tg.file("Hello, World!")
'
let id = tg put $artifact

# Cache.
tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
