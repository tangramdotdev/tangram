use ../../test.nu *

# Test metadata for a simple directory with files and no tag dependencies.

let local = spawn --name local
let remote = spawn --cloud --name remote

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
let id = tg --url $local.url checkin $path
tg --url $local.url index
let metadata = tg --url $local.url object metadata --pretty $id
snapshot --name metadata $metadata '
	{
	  "node": {
	    "size": 53,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 6,
	    "depth": 4,
	    "size": 317,
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
