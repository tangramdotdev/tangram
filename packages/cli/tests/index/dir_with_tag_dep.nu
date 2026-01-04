use ../../test.nu *

# Test metadata for a directory with a tagged dependency.

let local = spawn -n local
let remote = spawn -n remote

# Tag a dependency.
let dep_path = artifact {
	tangram.ts: '
		export default () => "dependency";
	'
}
tg -u $local.url tag dep $dep_path

# A directory containing a tangram.ts with a tag dependency should have solvable=true and solved=true.
let path = artifact {
	tangram.ts: '
		import dep from "dep";
		export default () => dep();
	'
}
let id = tg -u $local.url checkin $path
tg -u $local.url index
let metadata = tg -u $local.url object metadata --pretty $id
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

# Push to push and verify metadata matches.
tg -u $local.url remote put push $remote.url
tg -u $local.url push --remote push $id
tg -u $remote.url tag dep $dep_path
tg -u $remote.url index
let remote_metadata = tg -u $remote.url object metadata --pretty $id
assert equal $remote_metadata $metadata
