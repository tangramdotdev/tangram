use ../../test.nu *

# Test metadata for a simple directory with files and no tag dependencies.

let local = spawn -n local
let remote = spawn --cloud -n remote

let path = artifact {
	dir: {
		a.tg.ts: '
			export default "hello from a";
		'
		b.tg.ts: '
			export default "hello from b";
		'
	}
}
let id = tg -u $local.url checkin $path
tg -u $local.url index
let metadata = tg -u $local.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "node": {
	    "size": 51,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 6,
	    "depth": 4,
	    "size": 313,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Push to push and verify metadata matches.
tg -u $local.url remote put push $remote.url
tg -u $local.url push --remote push $id
tg -u $remote.url index
let remote_metadata = tg -u $remote.url object metadata --pretty $id
assert equal $remote_metadata $metadata
