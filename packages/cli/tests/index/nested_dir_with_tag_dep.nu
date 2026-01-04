use ../../test.nu *

# Test metadata for nested directories where an inner file has a tag dependency.

let local_server = spawn -n local
let push_server = spawn -n push

# Tag a dependency.
let dep_path = artifact {
	tangram.ts: '
		export default () => "dependency";
	'
}
tg -u $local_server.url tag dep $dep_path
let path = artifact {
	inner: {
		tangram.ts: '
			import dep from "dep";
			export default dep;
		'
	}
	tangram.ts: '
		import inner from "./inner";
		export default () => inner();
	'
}
let id = tg -u $local_server.url checkin $path
tg -u $local_server.url index
let metadata = tg -u $local_server.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "node": {
	    "size": 103,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 15,
	    "depth": 7,
	    "size": 1053,
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
