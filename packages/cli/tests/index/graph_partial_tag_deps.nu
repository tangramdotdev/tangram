use ../../test.nu *

# Test metadata for a graph where only one file has a tag dependency.

let local = spawn -n local
let remote = spawn --cloud -n remote

# Tag a dependency.
let dep_path = artifact {
	tangram.ts: '
		export default () => "dependency";
	'
}
tg -u $local.url tag dep $dep_path

# Graph with cyclic local imports where only one file in the cycle has a tag dep.
let path = artifact {
	a.tg.ts: '
		import b from "./b.tg.ts";
		import dep from "dep";
		export default () => b();
	'
	b.tg.ts: '
		import c from "./c.tg.ts";
		export default () => c();
	'
	c.tg.ts: '
		import a from "./a.tg.ts";
		export default () => "cycle";
	'
	tangram.ts: '
		import a from "./a.tg.ts";
		export default () => a();
	'
}
let id = tg -u $local.url checkin $path
tg -u $local.url index
let metadata = tg -u $local.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "node": {
	    "size": 229,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 17,
	    "depth": 6,
	    "size": 1787,
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
