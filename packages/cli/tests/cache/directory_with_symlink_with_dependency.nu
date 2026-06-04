use ../../test.nu *

# Caching a directory containing a symlink whose artifact is a dependency file writes the directory into the artifacts cache.

let server = spawn

# Create the artifact.
let artifact = '
	tg.directory({
		"foo": tg.symlink({
			"artifact": tg.file("bar")
		})
	})
'
let id = tg put $artifact

# Cache.
tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
