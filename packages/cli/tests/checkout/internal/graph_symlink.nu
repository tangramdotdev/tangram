use ../../../test.nu *

# Checking out a symlink defined through a graph node writes the symlink into the checkouts directory.

let server = server spawn

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
let id = tg put $artifact

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $server.checkout_directory '
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
