use ../../test.nu *

# Test metadata for a package with conflicting tag dependencies that cannot be solved.

let local = spawn -n local
let remote = spawn --cloud -n remote

# Create conflicting versions of a dependency.
let c1 = artifact {
	tangram.ts: ''
}
tg -u $local.url tag c/1.0.0 $c1

let c2 = artifact {
	tangram.ts: ''
}
tg -u $local.url tag c/2.0.0 $c2

# Create packages that require incompatible versions.
let a = artifact {
	tangram.ts: '
		import * as c from "c/^1"
	'
}
tg -u $local.url tag a/1.0.0 $a

let b = artifact {
	tangram.ts: '
		import * as c from "c/^2"
	'
}
tg -u $local.url tag b/1.0.0 $b

# A package that imports both, creating an unsolvable conflict.
let path = artifact {
	tangram.ts: '
		import * as a from "a/*";
		import * as b from "b/*";
	'
}
let id = tg -u $local.url checkin --unsolved-dependencies $path
tg -u $local.url index
let metadata = tg -u $local.url object metadata --pretty $id
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
	    "size": 644,
	    "solvable": true,
	    "solved": false,
	  },
	}
'

# Push to push and verify metadata matches.
tg -u $local.url remote put push $remote.url
tg -u $local.url push --remote push $id
tg -u $remote.url tag c/1.0.0 $c1
tg -u $remote.url tag c/2.0.0 $c2
tg -u $remote.url tag a/1.0.0 $a
tg -u $remote.url tag b/1.0.0 $b
tg -u $remote.url index
let remote_metadata = tg -u $remote.url object metadata --pretty $id
assert equal $remote_metadata $metadata
