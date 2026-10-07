use ../lib/test.nu *

# Indexing computes the expected process metadata locally and the metadata matches after pushing the process to a remote and indexing there.

let remote = server spawn --cloud --name remote
let local = server spawn --name local
tg remote put default $remote.url

let path = artifact {
	tangram.ts: r#'
		export default function () {}
	'#
}
let id = tg build --no-tokens --detach $path | referent node
tg wait $id

tg index

let metadata = tg process metadata $id | from json
let metadata = $metadata | update node.command_objects { reject size } | update subtree.command_objects { reject size }
let metadata = $metadata | to json --indent 2
snapshot --name local_metadata $metadata '
	{
	  "node": {
	    "command_objects": {
	      "count": 2,
	      "depth": 2,
	      "solvable": false,
	      "solved": true
	    },
	    "error_objects": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true
	    },
	    "log_objects": {
	      "count": 3,
	      "depth": 2,
	      "size": 110,
	      "solvable": false,
	      "solved": true
	    },
	    "output_objects": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true
	    }
	  },
	  "subtree": {
	    "command_objects": {
	      "count": 2,
	      "depth": 2,
	      "solvable": false,
	      "solved": true
	    },
	    "count": 1,
	    "error_objects": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true
	    },
	    "log_objects": {
	      "count": 3,
	      "depth": 2,
	      "size": 110,
	      "solvable": false,
	      "solved": true
	    },
	    "output_objects": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true
	    }
	  }
	}
'

tg push $id

tg --url $remote.url index

let remote_metadata = tg --url $remote.url metadata --pretty $id
snapshot --name remote_metadata $remote_metadata '
	{
	  "node": {
	    "error_objects": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true,
	    },
	    "output_objects": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true,
	    },
	  },
	  "subtree": {
	    "count": 1,
	    "error_objects": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true,
	    },
	    "output_objects": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true,
	    },
	  },
	}
'
