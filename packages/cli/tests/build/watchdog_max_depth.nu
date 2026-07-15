use ../../test.nu *

# The watchdog fails a build whose process tree exceeds the configured maximum depth and reports a maximum depth exceeded error.

# Configure watchdog with low max_depth and fast interval.
let server = spawn --config {
	watchdog: {
		max_depth: 2
	}
}

# Create a build that goes deeper than max_depth.
let path = artifact {
	tangram.ts: '
		export async function foo() {
			await tg.build(bar);
		}
		export async function bar() {
			await tg.build(baz);
		}
		export async function baz() {
			await tg.build(qux);
		}
		export async function qux() {
			return "qux";
		}
	'
}

let output = tg build ($path + '#foo') | complete
failure $output
snapshot --normalize-ids --redact $path $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> maximum depth exceeded

'
