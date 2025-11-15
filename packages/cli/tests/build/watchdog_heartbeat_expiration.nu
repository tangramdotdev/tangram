use std assert
use ../../test.nu *

# Configure watchdog with short TTL and fast interval.
let server = spawn -c {
	watchdog: {
		ttl: 0.1
		interval: 0.1
	}
}

# Create a long-running process.
let path = artifact {
	tangram.ts: '
		export let foo = async () => {
			await tg.sleep(2);
			return "done";
		};
	'
}

# Start the build.
let output = tg build ($path + '#foo') | complete
failure $output
assert ($output.stderr | str contains 'heartbeat expired') "the error should mention heartbeat expired"
