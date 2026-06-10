use ../../test.nu *

# Caching a file with a dependency on another file writes the file into the artifacts cache.

let server = spawn

# Create the artifact.
let artifact = '
	tg.file({
		"contents": "foo",
		"dependencies": {
			"bar": {
				"item": tg.file("bar")
			}
		}
	})
'
let id = tg put $artifact

# Cache.
tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
