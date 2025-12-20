use ../../test.nu *

let local_server = spawn -n local
let push_server = spawn -n push

# Create some test content.
let path = artifact {
	tangram.ts: r#'
		export default () => {};
	'#
}

# Build the module.
let output = tg -u $local_server.url build -d $path | from json

# Parse the process ID.
let id = $output.process

# Index.
tg -u $local_server.url index
let metadata = tg -u $local_server.url process metadata --pretty $id
snapshot -n local_metadata $metadata '
	{
	  "node": {
	    "command": {
	      "count": 3,
	      "depth": 3,
	      "size": 199,
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

tg -u $local_server.url remote put push $push_server.url
tg -u $local_server.url push --remote push $id
tg -u $push_server.url index
let push_metadata = tg -u $push_server.url metadata --pretty $id
snapshot -n remote_metadata $push_metadata '
	{
	  "node": {
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
