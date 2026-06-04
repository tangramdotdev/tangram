use ../../test.nu *

# Indexing computes the expected process metadata locally and the metadata matches after pushing the process to a remote and indexing there.

let remote = spawn --cloud --name push
let local = spawn --name local
tg remote put default $remote.url

let path = artifact {
	tangram.ts: r#'
		export default () => {};
	'#
}
let id = tg build --detach $path | str trim
tg wait $id

tg index

let metadata = tg process metadata $id | from json
let metadata = $metadata | update node.command { reject size } | update subtree.command { reject size }
let metadata = $metadata | to json --indent 2
snapshot --name local_metadata $metadata '
	{
	  "node": {
	    "command": {
	      "count": 3,
	      "depth": 3,
	      "solvable": false,
	      "solved": true
	    },
	    "error": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true
	    },
	    "log": {
	      "count": 1,
	      "depth": 1,
	      "size": 14,
	      "solvable": false,
	      "solved": true
	    },
	    "output": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true
	    }
	  },
	  "subtree": {
	    "command": {
	      "count": 3,
	      "depth": 3,
	      "solvable": false,
	      "solved": true
	    },
	    "count": 1,
	    "error": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true
	    },
	    "log": {
	      "count": 1,
	      "depth": 1,
	      "size": 14,
	      "solvable": false,
	      "solved": true
	    },
	    "output": {
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
	    "error": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true,
	    },
	    "output": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true,
	    },
	  },
	  "subtree": {
	    "count": 1,
	    "error": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true,
	    },
	    "output": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true,
	    },
	  },
	}
'
