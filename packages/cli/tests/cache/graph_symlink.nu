use ../../test.nu *

let server = spawn

# Create the artifact.
let artifact = '
	tg.symlink({
		"graph": tg.graph({
			"nodes": [
				{
					"kind": "symlink",
					"path": "/bin/sh"
				}
			]
		}),
		"node": 0
	})
'
let id = run tg put $artifact

# Cache.
run tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts") '
	{
	  "kind": "directory",
	  "entries": {
	    "sym_01j4terqyd7btz9tm8wsh4ybwavbkt8dnc512p9mewa140hsdg7ydg": {
	      "kind": "symlink",
	      "path": "/bin/sh"
	    }
	  }
	}
'
