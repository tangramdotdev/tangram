use ../../test.nu *

# Test metadata for a directory with mixed children (nested dir with tag dep and file without).

let local = spawn -n local
let remote = spawn --cloud -n remote

# Tag a dependency.
let dep_path = artifact {
	tangram.ts: '
		export default () => "dependency";
	'
}
tg -u $local.url tag dep $dep_path
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
let id = tg -u $local.url checkin $path
tg -u $local.url index
let metadata = tg -u $local.url object metadata --pretty $id
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

# Push to push and verify metadata matches.
tg -u $local.url remote put push $remote.url
tg -u $local.url push --remote push $id
tg -u $remote.url tag dep $dep_path
tg -u $remote.url index
let remote_metadata = tg -u $remote.url object metadata --pretty $id
assert equal $remote_metadata $metadata
