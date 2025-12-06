use ../../test.nu *

# Test metadata for a simple directory with files and no tag dependencies.

let local_server = spawn -n local
let push_server = spawn -n push

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
let id = run tg -u $local_server.url checkin $path
run tg -u $local_server.url index
let metadata = run tg -u $local_server.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "count": 6,
	  "depth": 4,
	  "self_solvable": false,
	  "self_solved": true,
	  "solvable": false,
	  "solved": true,
	  "weight": 303,
	}

'

# Push to push_server and verify metadata matches.
run tg -u $local_server.url remote put push $push_server.url
run tg -u $local_server.url push --remote push $id
run tg -u $push_server.url index
let push_metadata = run tg -u $push_server.url object metadata --pretty $id
assert equal $push_metadata $metadata
