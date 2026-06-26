use ../../test.nu *

# The watchdog fails a long-running build whose heartbeat expires under a short time-to-live and reports a heartbeat expired error.

# Configure the runner to stop refreshing the sandbox heartbeat during the build, and configure the watchdog with a short whole-second TTL and a fast interval.
let server = spawn --config {
	runner: {
		heartbeat_interval: 60
	}
	watchdog: {
		interval: 0.1
		ttl: 1
	}
}

# Create a long-running process.
let path = artifact {
	tangram.ts: '
		export async function foo() {
			await tg.sleep(4);
			return "done";
		}
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
