use ../../test.nu *

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
