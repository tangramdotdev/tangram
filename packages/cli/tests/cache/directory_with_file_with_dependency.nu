use ../../test.nu *

# Caching a directory containing a file with a dependency writes the directory into the artifacts cache.

let server = spawn

# Create the artifact.
let artifact = '
	tg.directory({
		"foo": tg.file({
			"contents": "foo",
			"dependencies": {
				"bar": {
					"item": tg.file("bar")
				}
			}
		})
	})
'
let id = tg put $artifact

# Cache.
tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
