use ../../../util/test.nu *

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
	    "sym_014caaz0r8jt1yhhrw579p379wnah3s8yrtver9wz6tcbtpn14vv3g": {
	      "kind": "symlink",
	      "path": "/bin/sh"
	    }
	  }
	}
'
