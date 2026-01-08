use ../../test.nu *

# Test metadata for a graph that imports an unsolved dependency.

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

# A graph that imports both, creating an unsolvable conflict.
let unsolved_path = artifact {
	a.tg.ts: '
		import * as a from "a/*";
		import "./b.tg.ts";
		export default () => "unsolvable";
	'
	b.tg.ts: '
    	import * as b from "b/*";
        import "./a.tg.ts";
    '
}
let unsolved_id = tg -u $local.url checkin --unsolved-dependencies $unsolved_path
tg -u $local.url tag unsolved/1.0.0 $unsolved_id

# Import the unsolved graph.
let path = artifact {
	tangram.ts: '
		import * as unsolved from "unsolved/*";
		export default tg.command(async () => unsolved.default());
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
	    "count": 13,
	    "depth": 7,
	    "size": 1241,
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
tg -u $remote.url tag unsolved/1.0.0 $unsolved_id
tg -u $remote.url index
let remote_metadata = tg -u $remote.url object metadata --pretty $id
assert equal $remote_metadata $metadata
