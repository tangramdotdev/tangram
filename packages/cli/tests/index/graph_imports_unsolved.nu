use ../../test.nu *

# Test metadata for a graph that imports an unsolved dependency.

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
let unsolved_id = tg --url $local.url checkin --unsolved-dependencies $unsolved_path
tg --url $local.url tag unsolved/1.0.0 $unsolved_id

# Import the unsolved graph.
let path = artifact {
	tangram.ts: '
		import * as unsolved from "unsolved/*";
		export default tg.command(async () => unsolved.default());
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
	    "count": 13,
	    "depth": 7,
	    "size": 1245,
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
tg --url $remote.url tag unsolved/1.0.0 $unsolved_id
tg --url $remote.url index
let remote_metadata = tg --url $remote.url object metadata --pretty $id
assert equal $remote_metadata $metadata
