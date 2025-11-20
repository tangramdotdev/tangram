use std assert
use ../../test.nu *

# Configure watchdog with low max_depth and fast interval.
let server = spawn -c {
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
