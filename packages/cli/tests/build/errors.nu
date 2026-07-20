use ../../test.nu *

# When a child process throws an error, both the child error and the parent's wrapping error carry the expected message, source, and stack information.

let server = spawn

let path = artifact {
	tangram.ts: r#'
		export default async function () {
			await tg.build(x);
		};

		export function x() {
			throw tg.error.sync("oops");
		}
	'#
}

# Build and get process ID.
let process_id = tg build --detach $path | str trim

# Wait for process to complete.
tg wait $process_id

# Get process data.
let process = tg get $process_id | from json

# Get error IDs from parent and child processes.
let parent_error = $process.error
let child_id = $process.children | first | get process
let child = tg get $child_id | from json
let child_error = $child.error

let output = tg get $parent_error --pretty
snapshot --normalize-ids $output '
	tg.error({
	  "message": "the child process failed",
	  "source": err_010000000000000000000000000000000000000000000000000000,
	  "stack": [
	    {
	      "file": {
	        "kind": "module",
	        "value": {
	          "kind": "ts",
	          "referent": {
	            "item": fil_010000000000000000000000000000000000000000000000000000,
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
	    "id": "pcs_0000000000000000000000000000",
	  },
	})
'

snapshot --normalize-ids (tg get $child_error --pretty) '
	tg.error({
	  "message": "oops",
	  "stack": [
	    {
	      "file": {
	        "kind": "module",
	        "value": {
	          "kind": "ts",
	          "referent": {
	            "item": fil_010000000000000000000000000000000000000000000000000000,
	          },
	        },
	      },
	      "range": {
	        "start": {
	          "line": 5,
	          "character": 16,
	        },
	        "end": {
	          "line": 5,
	          "character": 16,
	        },
	      },
	      "symbol": "x",
	    },
	  ],
	})
'
