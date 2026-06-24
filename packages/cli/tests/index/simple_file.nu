use ../../test.nu *

# Test metadata for a simple file with no dependencies.

let local = spawn --name local
let remote = spawn --cloud --name remote

let path = artifact {
	tangram.ts: '
		export default function () { return "hello"; }
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
	    "count": 3,
	    "depth": 3,
	    "size": 156,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Push to push and verify metadata matches.
tg --url $local.url remote put push $remote.url
tg --url $local.url push --remote=push $id
tg --url $remote.url index
let remote_metadata = tg --url $remote.url object metadata --pretty $id
assert equal $remote_metadata $metadata
