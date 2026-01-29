use ../../test.nu *

# Test that metadata propagates from child to parent through the update queue.

let local = spawn -n local

# Create and check in a child file first.
let child_path = artifact {
	tangram.ts: '
		export default () => "child";
	'
}
let child_id = tg -u $local.url checkin $child_path

# Index and verify the child metadata.
tg -u $local.url index
let child_metadata = tg -u $local.url object metadata --pretty $child_id
snapshot -n child_metadata $child_metadata '
	{
	  "node": {
	    "size": 60,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 3,
	    "depth": 3,
	    "size": 139,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Now create a parent directory that contains the child.
let parent_path = artifact {
	child: {
		tangram.ts: '
			export default () => "child";
		'
	}
	tangram.ts: '
		import child from "./child";
		export default () => child();
	'
}
let parent_id = tg -u $local.url checkin $parent_path

# Index and verify the parent metadata includes the child's metadata.
tg -u $local.url index
let parent_metadata = tg -u $local.url object metadata --pretty $parent_id
snapshot -n parent_metadata $parent_metadata '
	{
	  "node": {
	    "size": 105,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 9,
	    "depth": 5,
	    "size": 555,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Verify that child metadata is still correct after parent indexing.
let child_metadata_after = tg -u $local.url object metadata --pretty $child_id
assert equal $child_metadata_after $child_metadata
