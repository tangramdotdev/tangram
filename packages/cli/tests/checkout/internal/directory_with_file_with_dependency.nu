use ../../lib/test.nu *

# Checking out a directory containing a file with a dependency writes the directory into the checkouts directory.

let local = server spawn

# Create the artifact.
let artifact = '
	tg.directory({
		"foo": tg.file({
			"contents": "foo",
			"dependencies": {
				"bar": {
					"node": tg.file("bar")
				}
			}
		})
	})
'
let id = tg put $artifact

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $local.checkout_directory
