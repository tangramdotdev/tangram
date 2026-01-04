use ../../test.nu *

# Test metadata for nested directories where an inner file has a tag dependency.

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
let id = tg -u $local.url checkin $path
tg -u $local.url index
let metadata = tg -u $local.url object metadata --pretty $id
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

# Push to push and verify metadata matches.
tg -u $local.url remote put push $remote.url
tg -u $local.url push --remote push $id
tg -u $remote.url tag dep $dep_path
tg -u $remote.url index
let remote_metadata = tg -u $remote.url object metadata --pretty $id
assert equal $remote_metadata $metadata
