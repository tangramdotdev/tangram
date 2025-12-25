use ../../test.nu *

let remote = spawn -n push
let local = spawn -n local
tg remote put default $remote.url

let path = artifact {
	tangram.ts: r#'
		export default () => {};
	'#
}
let id = tg build -d $path | from json | get process
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
	      "depth": 3
	    },
	    "error": {
	      "count": 0,
	      "depth": 0,
	      "size": 0
	    },
	    "log": {
	      "count": 0,
	      "depth": 0,
	      "size": 0
	    },
	    "output": {
	      "count": 0,
	      "depth": 0,
	      "size": 0
	    }
	  },
	  "subtree": {
	    "command": {
	      "count": 3,
	      "depth": 3
	    },
	    "count": 1,
	    "error": {
	      "count": 0,
	      "depth": 0,
	      "size": 0
	    },
	    "log": {
	      "count": 0,
	      "depth": 0,
	      "size": 0
	    },
	    "output": {
	      "count": 0,
	      "depth": 0,
	      "size": 0
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
	    },
	    "log": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	    },
	    "output": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	    },
	  },
	  "subtree": {
	    "count": 1,
	    "error": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	    },
	    "log": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	    },
	    "output": {
	      "count": 0,
	      "depth": 0,
	      "size": 0,
	    },
	  },
	}
'
