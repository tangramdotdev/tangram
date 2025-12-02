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
run tg -u $local_server.url tag dep $dep_path
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
let id = run tg -u $local_server.url checkin $path
run tg -u $local_server.url index
let metadata = run tg -u $local_server.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "count": 19,
	  "depth": 7,
	  "self_solvable": false,
	  "self_solved": true,
	  "solvable": true,
	  "solved": true,
	  "weight": 1396,
	}

'

# Push to push_server and verify metadata matches.
run tg -u $local_server.url remote put push $push_server.url
run tg -u $local_server.url push --remote push $id
run tg -u $push_server.url tag dep $dep_path
run tg -u $push_server.url index
let push_metadata = run tg -u $push_server.url object metadata --pretty $id
assert equal $push_metadata $metadata
