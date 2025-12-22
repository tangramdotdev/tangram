use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: r#'
		export default async function () {
			await tg.run(x);
		};

		export function x() {
			throw tg.error("oops");
		}
	'#
}

# Build and get process ID.
let output = tg build -d $path | from json
let process_id = $output.process

# Wait for process to complete.
tg wait $process_id

# Get process data.
let process = tg get $process_id | from json

# Get error IDs from parent and child processes.
let parent_error = $process.error
let child_id = $process.children | first | get item
let child = tg get $child_id | from json
let child_error = $child.error

def normalize [] {
	str replace -ar 'err_[0-9a-z]+' 'ERROR'
		| str replace -ar 'pcs_[0-9a-z]+' 'PROCESS'
		| str replace -ar 'fil_[0-9a-z]+' 'FILE'
}
let output = tg get $parent_error --pretty | normalize
snapshot $output '
	tg.error({
	  "message": "the child process failed",
	  "source": ERROR,
	  "stack": [
	    {
	      "file": {
	        "kind": "module",
	        "value": {
	          "kind": "ts",
	          "referent": {
	            "item": FILE,
	          },
	        },
	      },
	      "range": {
	        "start": {
	          "line": 1,
	          "character": 1,
	        },
	        "end": {
	          "line": 1,
	          "character": 1,
	        },
	      },
	      "symbol": "default",
	    },
	  ],
	  "values": {
	    "id": "PROCESS",
	  },
	})
'

snapshot (tg get $child_error --pretty | normalize) '
	tg.error({
	  "message": "oops",
	  "stack": [
	    {
	      "file": {
	        "kind": "module",
	        "value": {
	          "kind": "ts",
	          "referent": {
	            "item": FILE,
	          },
	        },
	      },
	      "range": {
	        "start": {
	          "line": 5,
	          "character": 10,
	        },
	        "end": {
	          "line": 5,
	          "character": 10,
	        },
	      },
	      "symbol": "x",
	    },
	  ],
	})
'
