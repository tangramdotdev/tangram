use ../../test.nu *

let server = spawn

# Create the artifact.
let artifact = '
	tg.symlink({
		"path": "/bin/sh"
	})
'
let id = run tg put $artifact

# Cache.
run tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
