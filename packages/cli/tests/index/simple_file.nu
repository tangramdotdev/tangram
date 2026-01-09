use ../../test.nu *

# Test metadata for a simple file with no dependencies.

let local = spawn -n local
let remote = spawn --cloud -n remote

let path = artifact {
	tangram.ts: '
		export default () => "hello";
	'
}
let id = tg -u $local.url checkin $path
tg -u $local.url index
let metadata = tg -u $local.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "node": {
	    "size": 60,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 3,
	    "depth": 3,
	    "size": 139,
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
