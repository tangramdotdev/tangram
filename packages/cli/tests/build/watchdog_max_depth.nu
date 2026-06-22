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
		export let foo = async () => {
			await tg.build(bar);
		};
		export let bar = async () => {
			await tg.build(baz);
		};
		export let baz = async () => {
			await tg.build(qux);
		};
		export let qux = async () => {
			return "qux";
		};
	'
}

let output = tg build ($path + '#foo') | complete
failure $output
snapshot ($output.stderr | redact $path | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> maximum depth exceeded

'
