use ../../lib/test.nu *

# Checking out a symlink defined through a graph node writes the symlink into the checkouts directory.

let local = server spawn

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
let id = tg put --no-tokens $artifact | referent node

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $local.checkout_directory '
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
