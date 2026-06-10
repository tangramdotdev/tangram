use ../../test.nu *

# Blobs have correct metadata.

let local = spawn --name local
let remote = spawn --cloud --name remote

# Create a blob by writing data.
let temp_file = mktemp --tmpdir
"hello, world!\n" | save --force $temp_file

let id = cat $temp_file | tg --url $local.url write
tg --url $local.url index
let metadata = tg --url $local.url object metadata --pretty $id
snapshot --name metadata $metadata '
	{
	  "node": {
	    "size": 15,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 1,
	    "depth": 1,
	    "size": 15,
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

