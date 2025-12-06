use ../../test.nu *

# Test metadata for a graph with conflicting version requirements that cannot be solved.

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
let id = run tg -u $local_server.url checkin --unsolved-dependencies $path
run tg -u $local_server.url index

let metadata = run tg -u $local_server.url object metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "count": 10,
	  "depth": 5,
	  "self_solvable": false,
	  "self_solved": true,
	  "solvable": true,
	  "solved": false,
	  "weight": 911,
	}

'

# Get the file a.tg.ts metadata.
let file_id = run tg -u $local_server.url checkin --unsolved-dependencies ($path | path join "a.tg.ts")
run tg -u $local_server.url index

let file_metadata = run tg -u $local_server.url object metadata --pretty $file_id
snapshot -n file_metadata $file_metadata '
	{
	  "count": 10,
	  "depth": 5,
	  "self_solvable": false,
	  "self_solved": true,
	  "solvable": true,
	  "solved": false,
	  "weight": 840,
	}

'

# Get the file b.tg.ts metadata.
let file_b_id = run tg -u $local_server.url checkin --unsolved-dependencies ($path | path join "b.tg.ts")
run tg -u $local_server.url index

let file_b_metadata = run tg -u $local_server.url object metadata --pretty $file_b_id
snapshot -n file_b_metadata $file_b_metadata '
	{
	  "count": 10,
	  "depth": 5,
	  "self_solvable": false,
	  "self_solved": true,
	  "solvable": true,
	  "solved": false,
	  "weight": 840,
	}

'

# Get the graph id and check its metadata.
let file_obj = run tg -u $local_server.url get $file_id
let graph_id = $file_obj | parse --regex '"graph":(gph_[a-z0-9]+)' | get capture0 | first
run tg -u $local_server.url index

let graph_metadata = run tg -u $local_server.url object metadata --pretty $graph_id
snapshot -n graph_metadata $graph_metadata '
	{
	  "count": 9,
	  "depth": 4,
	  "self_solvable": true,
	  "self_solved": true,
	  "solvable": true,
	  "solved": false,
	  "weight": 789,
	}

'

# Push to push_server and verify metadata matches.
run tg -u $local_server.url remote put push $push_server.url
run tg -u $local_server.url push --remote push $id
run tg -u $push_server.url tag c/1.0.0 $c1
run tg -u $push_server.url tag c/2.0.0 $c2
run tg -u $push_server.url tag a/1.0.0 $a
run tg -u $push_server.url tag b/1.0.0 $b
run tg -u $push_server.url index
let push_metadata = run tg -u $push_server.url object metadata --pretty $id
assert equal $push_metadata $metadata
