use ../../test.nu *

# Test metadata for a graph where only one file has a tag dependency.

let local = spawn --name local
let remote = spawn --cloud --name remote

# Tag a dependency.
let dep_path = artifact {
	tangram.ts: '
		export default function () { return "dependency"; }
	'
}
tg --url $local.url tag dep $dep_path

# Graph with cyclic local imports where only one file in the cycle has a tag dep.
let path = artifact {
	a.tg.ts: '
		import b from "./b.tg.ts";
		import dep from "dep";
		export default function () { return b(); }
	'
	b.tg.ts: '
		import c from "./c.tg.ts";
		export default function () { return c(); }
	'
	c.tg.ts: '
		import a from "./a.tg.ts";
		export default function () { return "cycle"; }
	'
	tangram.ts: '
		import a from "./a.tg.ts";
		export default function () { return a(); }
	'
}
let id = tg --url $local.url checkin $path
tg --url $local.url index
let metadata = tg --url $local.url object metadata --pretty $id
snapshot --name metadata $metadata '
	{
	  "node": {
	    "size": 231,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 17,
	    "depth": 6,
	    "size": 1946,
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
