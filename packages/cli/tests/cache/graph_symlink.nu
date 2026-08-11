use ../../test.nu *

# Caching a symlink defined through a graph node writes the symlink into the artifacts cache.

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
let id = tg put $artifact

# Cache.
tg cache $id

# Snapshot.
snapshot --path $server.cache_directory '
	{
	  "kind": "directory",
	  "entries": {
	    "sym_014gcwf6chcc0egb4wy71082hrqsd743xnds4m9vaapk883z6tjg60": {
	      "kind": "symlink",
	      "path": "/bin/sh"
	    }
	  }
	}
'
