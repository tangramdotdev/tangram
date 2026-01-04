use ../../test.nu *

# Test metadata for a directory with a tagged dependency.

let local_server = spawn -n local
let push_server = spawn -n push

# Tag a dependency.
let dep_path = artifact {
	tangram.ts: '
		export default () => "dependency";
	'
}
tg -u $local_server.url tag dep $dep_path

# A directory containing a tangram.ts with a tag dependency should have solvable=true and solved=true.
let path = artifact {
	tangram.ts: '
		import dep from "dep";
		export default () => dep();
	'
}
let id = tg -u $local_server.url checkin $path
tg -u $local_server.url index
let metadata = tg -u $local_server.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "node": {
	    "size": 58,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 6,
	    "depth": 5,
	    "size": 397,
	    "solvable": true,
	    "solved": true,
	  },
	}
'

# Push to push_server and verify metadata matches.
tg -u $local_server.url remote put push $push_server.url
tg -u $local_server.url push --remote push $id
tg -u $push_server.url tag dep $dep_path
tg -u $push_server.url index
let push_metadata = tg -u $push_server.url object metadata --pretty $id
assert equal $push_metadata $metadata
