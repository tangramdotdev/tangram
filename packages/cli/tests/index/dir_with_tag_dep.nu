use ../../test.nu *

# Test metadata for a directory with a tagged dependency.

let local = spawn --name local
let remote = spawn --cloud --name remote

# Tag a dependency.
let dep_path = artifact {
	tangram.ts: '
		export default function () { return "dependency"; }
	'
}
tg --url $local.url tag dep $dep_path

# A directory containing a tangram.ts with a tag dependency should have solvable=true and solved=true.
let path = artifact {
	tangram.ts: '
		import dep from "dep";
		export default function () { return dep(); }
	'
}
let id = tg --url $local.url checkin $path
tg --url $local.url index
let metadata = tg --url $local.url object metadata --pretty $id
snapshot --name metadata $metadata '
	{
	  "node": {
	    "size": 60,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 6,
	    "depth": 5,
	    "size": 435,
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
