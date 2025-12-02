use ../../test.nu *

# Test that blobs have correct metadata.

let local_server = spawn -n local
let push_server = spawn -n push

# Create a blob by writing data.
let temp_file = mktemp -t
"hello, world!\n" | save -f $temp_file

let id = cat $temp_file | tg -u $local_server.url write
run tg -u $local_server.url index
let metadata = run tg -u $local_server.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "count": 1,
	  "depth": 1,
	  "self_solvable": false,
	  "self_solved": true,
	  "solvable": false,
	  "solved": true,
	  "weight": 15,
	}

'

# Push to push_server and verify metadata matches.
run tg -u $local_server.url remote put push $push_server.url
run tg -u $local_server.url push --remote push $id
run tg -u $push_server.url index
let push_metadata = run tg -u $push_server.url object metadata --pretty $id
assert equal $push_metadata $metadata

