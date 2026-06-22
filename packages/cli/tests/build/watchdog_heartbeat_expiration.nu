use ../../test.nu *

# The watchdog fails a long-running build whose heartbeat expires under a short time-to-live and reports a heartbeat expired error.

# Configure watchdog with short TTL and fast interval.
let server = spawn --config {
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
snapshot ($output.stderr | redact $path | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> heartbeat expired

'
