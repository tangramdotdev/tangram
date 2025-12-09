use ../../test.nu *

# Test metadata for a graph with cyclic local imports but no tag dependencies.

let local_server = spawn -n local
let push_server = spawn -n push

let path = artifact {
	a.tg.ts: '
		import b from "./b.tg.ts";
	'
	b.tg.ts: '
		import a from "./a.tg.ts";
	'
}
let id = tg -u $local_server.url checkin $path
tg -u $local_server.url index
let metadata = tg -u $local_server.url object metadata --pretty $id
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
	    "size": 346,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Push to push_server and verify metadata matches.
tg -u $local_server.url remote put push $push_server.url
tg -u $local_server.url push --remote push $id
tg -u $push_server.url index
let push_metadata = tg -u $push_server.url object metadata --pretty $id
assert equal $push_metadata $metadata
