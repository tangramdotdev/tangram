use ../../test.nu *

# Test metadata for a simple directory with files and no tag dependencies.

let local_server = spawn -n local
let push_server = spawn -n push

let path = artifact {
	dir: {
		a.tg.ts: '
			export default "hello from a";
		'
		b.tg.ts: '
			export default "hello from b";
		'
	}
}
let id = tg -u $local_server.url checkin $path
tg -u $local_server.url index
let metadata = tg -u $local_server.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "node": {
	    "size": 51,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 6,
	    "depth": 4,
	    "size": 313,
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
