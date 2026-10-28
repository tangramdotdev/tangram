use ../lib/test.nu *

# Looking up a sandbox or process that was cleaned reports that it is not found without waiting for a control read.

let local = server spawn --config {
	advanced: { checkpoints: true },
	control: { read_timeout: 60 },
	runner: { process_state_ttl: 0, sandbox_state_ttl: 0 },
}

let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.file("output");
		}
	'
}

# Run a process in a sandbox, and wait for the runner to discard the state of both.
let process_removed = checkpoint watch runner.process.state.removed
let sandbox_removed = checkpoint watch runner.sandbox.state.removed
let process = tg process spawn --no-tokens --sandbox $path | referent node
tg wait --source=index $process | ignore
let sandbox = tg process get $process | from json | get sandbox | split row '?' | first
tg wait --source=index $sandbox | ignore
checkpoint pass $process_removed | ignore
checkpoint unwatch $process_removed
checkpoint pass $sandbox_removed | ignore
checkpoint unwatch $sandbox_removed

# Clean the sandbox and the process.
tg clean

# Each lookup reports that the sandbox or process is not found, rather than waiting out the control read timeout.
let output = timeout 20s tg sandbox get $sandbox | complete
failure $output
assert ($output.stderr | str contains 'failed to find the sandbox') $output.stderr
let output = timeout 20s tg process get $process | complete
failure $output
assert ($output.stderr | str contains 'failed to get the process') $output.stderr
let output = timeout 20s tg process children $process | complete
failure $output
assert ($output.stderr | str contains 'failed to get the process children') $output.stderr
let socket = $local.url | str replace 'http+unix://' '' | url decode
let status = http get --allow-errors --full --max-time 20sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/status?timeout=0'
assert equal $status.status 404
let processes = http get --allow-errors --full --max-time 20sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/processes?timeout=0'
assert equal $processes.status 404
