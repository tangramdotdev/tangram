use ../../test.nu *

# Test metadata for a directory with mixed children (nested dir with tag dep and file without).

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
	with_dep: {
		tangram.ts: '
			import dep from "dep";
			export default dep;
		'
	}
	without_dep.tg.ts: '
		export default () => "no dependency";
	'
	tangram.ts: '
		import withDep from "./with_dep";
		import withoutDep from "./without_dep.tg.ts";
		export default () => withDep();
	'
}
let id = tg -u $local_server.url checkin $path
tg -u $local_server.url index
let metadata = tg -u $local_server.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "node": {
	    "size": 163,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 19,
	    "depth": 7,
	    "size": 1431,
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
