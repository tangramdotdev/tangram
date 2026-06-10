use ../../test.nu *

# Caching a symlink with an absolute target path writes the symlink into the artifacts cache.

let server = spawn

# Create the artifact.
let artifact = '
	tg.symlink({
		"path": "/bin/sh"
	})
'
let id = tg put $artifact

# Cache.
tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
