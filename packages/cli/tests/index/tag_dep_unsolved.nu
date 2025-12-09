use ../../test.nu *

# Test metadata for a package with conflicting tag dependencies that cannot be solved.

let local_server = spawn -n local
let push_server = spawn -n push

# Create conflicting versions of a dependency.
let c1 = artifact {
	tangram.ts: ''
}
tg -u $local_server.url tag c/1.0.0 $c1

let c2 = artifact {
	tangram.ts: ''
}
tg -u $local_server.url tag c/2.0.0 $c2

# Create packages that require incompatible versions.
let a = artifact {
	tangram.ts: '
		import * as c from "c/^1"
	'
}
tg -u $local_server.url tag a/1.0.0 $a

let b = artifact {
	tangram.ts: '
		import * as c from "c/^2"
	'
}
tg -u $local_server.url tag b/1.0.0 $b

# A package that imports both, creating an unsolvable conflict.
let path = artifact {
	tangram.ts: '
		import * as a from "a/*";
		import * as b from "b/*";
	'
}
let id = tg -u $local_server.url checkin --unsolved-dependencies $path
tg -u $local_server.url index
let metadata = tg -u $local_server.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "node": {
	    "size": 58,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 9,
	    "depth": 5,
	    "size": 633,
	    "solvable": true,
	    "solved": false,
	  },
	}
'

# Push to push_server and verify metadata matches.
tg -u $local_server.url remote put push $push_server.url
tg -u $local_server.url push --remote push $id
tg -u $push_server.url tag c/1.0.0 $c1
tg -u $push_server.url tag c/2.0.0 $c2
tg -u $push_server.url tag a/1.0.0 $a
tg -u $push_server.url tag b/1.0.0 $b
tg -u $push_server.url index
let push_metadata = tg -u $push_server.url object metadata --pretty $id
assert equal $push_metadata $metadata
