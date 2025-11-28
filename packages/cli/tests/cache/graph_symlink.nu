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
		"index": 0,
		"kind": "symlink"
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
	    "sym_01ajczwn8gdjcpjn0fcf2re3qjmzga18cda8hxjn7dgmcyywv5p240": {
	      "kind": "symlink",
	      "path": "/bin/sh"
	    }
	  }
	}
'
