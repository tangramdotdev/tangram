use ../../test.nu *

let server = spawn

# Create the artifact.
let artifact = '
	tg.file({
		"graph": tg.graph({
			"nodes": [
				{
					"kind": "file",
					"contents": tg.blob("Hello, World!")
				}
			]
		}),
		"index": 0,
		"kind": "file"
	})
'
let id = run tg put $artifact

# Cache.
run tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
