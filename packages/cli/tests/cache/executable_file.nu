use ../../test.nu *

# Caching an executable file writes the file into the artifacts cache with its executable bit preserved.

let server = spawn

# Create the artifact.
let artifact = '
	tg.file({
		"contents": "Hello, World!",
		"executable": true
	})
'
let id = tg put $artifact

# Cache.
tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
