use ../../test.nu *

# Test metadata for a graph with cyclic local imports but no tag dependencies.

let local = spawn -n local
let remote = spawn -n remote

let path = artifact {
	a.tg.ts: '
		import b from "./b.tg.ts";
	'
	b.tg.ts: '
		import a from "./a.tg.ts";
	'
}
let id = tg -u $local.url checkin $path
tg -u $local.url index
let metadata = tg -u $local.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "node": {
	    "size": 122,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 4,
	    "depth": 3,
	    "size": 356,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Push to push and verify metadata matches.
tg -u $local.url remote put push $remote.url
tg -u $local.url push --remote push $id
tg -u $remote.url index
let remote_metadata = tg -u $remote.url object metadata --pretty $id
assert equal $remote_metadata $metadata
