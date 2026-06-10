use ../../test.nu *

# Test metadata for a graph with cyclic local imports but no tag dependencies.

let local = spawn --name local
let remote = spawn --cloud --name remote

let path = artifact {
	a.tg.ts: '
		import b from "./b.tg.ts";
	'
	b.tg.ts: '
		import a from "./a.tg.ts";
	'
}
let id = tg --url $local.url checkin $path
tg --url $local.url index
let metadata = tg --url $local.url object metadata --pretty $id
snapshot --name metadata $metadata '
	{
	  "node": {
	    "size": 124,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 4,
	    "depth": 3,
	    "size": 358,
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
