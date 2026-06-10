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
assert ($output.stderr | str contains 'maximum depth exceeded') "the error should mention maximum depth exceeded"
