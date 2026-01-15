use ../../test.nu *

# Test metadata for a graph with conflicting version requirements that cannot be solved.

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
let path = artifact {
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
let id = tg -u $local.url checkin --unsolved-dependencies $path
tg -u $local.url index

let metadata = tg -u $local.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "node": {
	    "size": 122,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 10,
	    "depth": 5,
	    "size": 911,
	    "solvable": true,
	    "solved": false,
	  },
	}
'

# Get the file a.tg.ts metadata.
let file_id = tg -u $local.url checkin --unsolved-dependencies ($path | path join "a.tg.ts")
tg -u $local.url index

let file_metadata = tg -u $local.url object metadata --pretty $file_id
snapshot -n file_metadata $file_metadata '
	{
	  "node": {
	    "size": 51,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 10,
	    "depth": 5,
	    "size": 840,
	    "solvable": true,
	    "solved": false,
	  },
	}
'

# Get the file b.tg.ts metadata.
let file_b_id = tg -u $local.url checkin --unsolved-dependencies ($path | path join "b.tg.ts")
tg -u $local.url index

let file_b_metadata = tg -u $local.url object metadata --pretty $file_b_id
snapshot -n file_b_metadata $file_b_metadata '
	{
	  "node": {
	    "size": 51,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 10,
	    "depth": 5,
	    "size": 840,
	    "solvable": true,
	    "solved": false,
	  },
	}
'

# Get the graph id and check its metadata.
let file_obj = tg -u $local.url get $file_id
let graph_id = $file_obj | parse --regex '"graph":(gph_[a-z0-9]+)' | get capture0 | first
tg -u $local.url index

let graph_metadata = tg -u $local.url object metadata --pretty $graph_id
snapshot -n graph_metadata $graph_metadata '
	{
	  "node": {
	    "size": 376,
	    "solvable": true,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 9,
	    "depth": 4,
	    "size": 789,
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
