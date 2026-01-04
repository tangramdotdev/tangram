use ../../test.nu *

# Test metadata for a graph with cyclic local imports and a tag dependency.

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
	a.tg.ts: '
		import b from "./b.tg.ts";
		import dep from "dep";
		export default () => b();
	'
	b.tg.ts: '
		import a from "./a.tg.ts";
		export default () => "cycle";
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
	    "count": 7,
	    "depth": 5,
	    "size": 671,
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
