use ../../test.nu *

# Test that blobs have correct metadata.

let local_server = spawn -n local
let push_server = spawn -n push

# Create a blob by writing data.
let temp_file = mktemp -t
"hello, world!\n" | save -f $temp_file

let id = cat $temp_file | tg -u $local_server.url write
tg -u $local_server.url index
let metadata = tg -u $local_server.url object metadata --pretty $id
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

# Push to push_server and verify metadata matches.
tg -u $local_server.url remote put push $push_server.url
tg -u $local_server.url push --remote push $id
tg -u $push_server.url index
let push_metadata = tg -u $push_server.url object metadata --pretty $id
assert equal $push_metadata $metadata

