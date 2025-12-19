use ../../test.nu *

let local_server = spawn -n local

# Create some test content.
let path = artifact {
	tangram.ts: r#'
		export default () => {};
	'#
}

# Build the module.
let output = tg build -d $path | from json

# Parse the process ID.
let id = $output.process

# Index.
tg -u $local_server.url index
let metadata = tg -u $local_server.url process metadata --pretty $id
snapshot -n metadata $metadata '
	{
	  "node": {
	    "command": {
	      "count": 3,
	      "depth": 3,
	      "size": 197,
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
	      "size": 197,
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
	    }
	  },
	}
'


