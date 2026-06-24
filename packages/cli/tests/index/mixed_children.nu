use ../../test.nu *

# Test metadata for a directory with mixed children (nested dir with tag dep and file without).

let local = spawn --name local
let remote = spawn --cloud --name remote

# Tag a dependency.
let dep_path = artifact {
	tangram.ts: '
		export default function () { return "dependency"; }
	'
}
tg --url $local.url tag dep $dep_path
let path = artifact {
	with_dep: {
		tangram.ts: '
			import dep from "dep";
			export default dep;
		'
	}
	without_dep.tg.ts: '
		export default function () { return "no dependency"; }
	'
	tangram.ts: '
		import withDep from "./with_dep";
		import withoutDep from "./without_dep.tg.ts";
		export default function () { return withDep(); }
	'
}
let id = tg --url $local.url checkin $path
tg --url $local.url index
let metadata = tg --url $local.url object metadata --pretty $id
snapshot --name metadata $metadata '
	{
	  "node": {
	    "size": 165,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 19,
	    "depth": 7,
	    "size": 1526,
	    "solvable": true,
	    "solved": true,
	  },
	}
'

# Push to push and verify metadata matches.
tg --url $local.url remote put push $remote.url
tg --url $local.url push --remote=push $id
tg --url $remote.url tag dep $dep_path
tg --url $remote.url index
let remote_metadata = tg --url $remote.url object metadata --pretty $id
assert equal $remote_metadata $metadata
