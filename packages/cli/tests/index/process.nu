use ../../test.nu *

let remote = spawn --cloud -n push
let local = spawn -n local
tg remote put default $remote.url

let path = artifact {
	tangram.ts: r#'
		export default () => {};
	'#
}
let id = tg build -d $path | str trim
tg wait $id

tg index
let metadata = tg process metadata $id | from json
let metadata = $metadata | update node.command { reject size } | update subtree.command { reject size }
let metadata = $metadata | to json -i 2
snapshot -n local_metadata $metadata '
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
	      "count": 0,
	      "depth": 0,
	      "size": 0,
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
	      "count": 0,
	      "depth": 0,
	      "size": 0,
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
tg -u $remote.url index
let remote_metadata = tg -u $remote.url metadata --pretty $id
snapshot -n remote_metadata $remote_metadata '
	{
	  "node": {
	    "error": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	      "solvable": false,
	      "solved": true,
	    },
	    "log": {
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
	    "log": {
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
