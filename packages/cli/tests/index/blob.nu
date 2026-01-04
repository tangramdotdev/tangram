use ../../test.nu *

# Test that blobs have correct metadata.

let local = spawn -n local
let remote = spawn --cloud -n remote

# Create a blob by writing data.
let temp_file = mktemp -t
"hello, world!\n" | save -f $temp_file

let id = cat $temp_file | tg -u $local.url write
tg -u $local.url index
let metadata = tg -u $local.url object metadata --pretty $id
snapshot -n metadata $metadata '
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
tg -u $local.url remote put push $remote.url
tg -u $local.url push --remote push $id
tg -u $remote.url index
let remote_metadata = tg -u $remote.url object metadata --pretty $id
assert equal $remote_metadata $metadata

