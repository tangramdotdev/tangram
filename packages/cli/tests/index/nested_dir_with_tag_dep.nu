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
run tg -u $local_server.url tag dep $dep_path
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
let id = run tg -u $local_server.url checkin $path
run tg -u $local_server.url index
let metadata = run tg -u $local_server.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "count": 15,
	  "depth": 7,
	  "self_solvable": false,
	  "self_solved": true,
	  "solvable": true,
	  "solved": true,
	  "weight": 1028,
	}

'

# Push to push_server and verify metadata matches.
run tg -u $local_server.url remote put push $push_server.url
run tg -u $local_server.url push --remote push $id
run tg -u $push_server.url tag dep $dep_path
run tg -u $push_server.url index
let push_metadata = run tg -u $push_server.url object metadata --pretty $id
assert equal $push_metadata $metadata
