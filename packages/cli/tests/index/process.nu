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
let metadata = tg process metadata --pretty $id
snapshot -n local_metadata $metadata '
	{
	  "node": {
	    "command": {
	      "count": 3,
	      "depth": 3,
	      "size": 199,
	    },
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
	    "command": {
	      "count": 3,
	      "depth": 3,
	      "size": 199,
	    },
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
