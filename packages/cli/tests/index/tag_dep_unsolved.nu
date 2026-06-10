use ../../test.nu *

# Test metadata for a package with conflicting tag dependencies that cannot be solved.

let local = spawn --name local
let remote = spawn --cloud --name remote

# Create conflicting versions of a dependency.
let c1 = artifact {
	tangram.ts: ''
}
tg --url $local.url tag c/1.0.0 $c1

let c2 = artifact {
	tangram.ts: ''
}
tg --url $local.url tag c/2.0.0 $c2

# Create packages that require incompatible versions.
let a = artifact {
	tangram.ts: '
		import * as c from "c/^1"
	'
}
tg --url $local.url tag a/1.0.0 $a

let b = artifact {
	tangram.ts: '
		import * as c from "c/^2"
	'
}
tg --url $local.url tag b/1.0.0 $b

# A package that imports both, creating an unsolvable conflict.
let path = artifact {
	tangram.ts: '
		import * as a from "a/*";
		import * as b from "b/*";
	'
}
let id = tg --url $local.url checkin --unsolved-dependencies $path
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
	    "count": 9,
	    "depth": 5,
	    "size": 650,
	    "solvable": true,
	    "solved": false,
	  },
	}
'

# Push to push and verify metadata matches.
tg --url $local.url remote put push $remote.url
tg --url $local.url push --remote=push $id
tg --url $remote.url tag c/1.0.0 $c1
tg --url $remote.url tag c/2.0.0 $c2
tg --url $remote.url tag a/1.0.0 $a
tg --url $remote.url tag b/1.0.0 $b
tg --url $remote.url index
let remote_metadata = tg --url $remote.url object metadata --pretty $id
assert equal $remote_metadata $metadata
