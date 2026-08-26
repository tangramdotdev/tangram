use ../../test.nu *

# A stdin write that is in flight when a cancelled process's sandbox is destroyed returns EOF instead of repeatedly retrying at the same position.

let server = server spawn --config {
	advanced: { checkpoints: true },
	runner: { process_state_ttl: 1 },
	tracing: { stderr_format: 'json' },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(60);
		}
	',
}

let attach_watch = (
	tg checkpoint watch process.wait.attach
	| from json
	| get watch
)
let stdin_watch = (
	tg checkpoint watch runner.process.control.stdin.write
	| from json
	| get watch
)
let destroyed_watch = (
	tg checkpoint watch runner.sandbox.destroyed
	| from json
	| get watch
)
let retention_watch = (
	tg checkpoint watch runner.process.control.retention.finished
	| from json
	| get watch
)

# Hold the client's stdin open with a pipe so that it does not close the process's stdin on its own.
let fifo_directory = mktemp -d
let fifo_path = $fifo_directory | path join 'stdin'
^mkfifo $fifo_path
let trigger_path = $fifo_directory | path join 'close'
let pid_path = mktemp
let tangram = which tg | where type == external | get path | first
let writer = job spawn {
	^bash -c 'exec 3> "$1"; while [ ! -e "$2" ]; do sleep 0.05; done; exec 3>&-' _ $fifo_path $trigger_path
}
let run = job spawn {
	let job_id = job id
	let output = (
		^bash -c 'echo "$$" > "$1"; exec "$2" run --sandbox --stdin pipe "$3" < "$4"' _ $pid_path $tangram $path $fifo_path
		| complete
	)
	$output | job send --tag $job_id 0
}

wait_until {
	(open --raw $pid_path | str trim | str length) > 0
} "tg run should publish its process ID"
let pid = open --raw $pid_path | str trim | into int

# Get the process while it is still running.
let hit = timeout 10s tg checkpoint wait process.wait.attach $attach_watch 0 | from json
let process = $hit.params.process
tg checkpoint continue process.wait.attach $attach_watch 0
tg checkpoint unwatch process.wait.attach $attach_watch

# Write to the process's stdin and hold the write in the runner.
let write = job spawn {
	let job_id = job id
	let output = (
		^bash -c 'printf data | "$1" process stdio write "$2" --stream stdin' _ $tangram $process
		| complete
	)
	$output | job send --tag $job_id 0
}
let hit = timeout 10s tg checkpoint wait runner.process.control.stdin.write $stdin_watch 0 | from json
assert equal $hit.params.close "false" "the held request should be a stdin write"

# Disconnect the client so that the wait lease is dropped and the sandbox is destroyed while the sandbox process is still running.
kill --signal 15 $pid
let output = timeout 10s tg checkpoint wait runner.sandbox.destroyed $destroyed_watch 0 | complete
success $output "the sandbox should be destroyed while the stdin write is held"
tg checkpoint continue runner.sandbox.destroyed $destroyed_watch 0
tg checkpoint unwatch runner.sandbox.destroyed $destroyed_watch

# Hold the retention window open so that the held write is not aborted before it is serviced.
let output = timeout 10s tg checkpoint wait runner.process.control.retention.finished $retention_watch 0 | complete
success $output "process control should reach the end of its retention window"

# Release the write, which is now serviced after the sandbox connection is gone.
tg checkpoint continue runner.process.control.stdin.write $stdin_watch 0
tg checkpoint unwatch runner.process.control.stdin.write $stdin_watch

let output = job recv --tag $write --timeout 10sec
if $output == null {
	error make { msg: 'the stdin write did not complete' }
}
success $output "a stdin write serviced after the sandbox is destroyed must return EOF"

tg checkpoint continue runner.process.control.retention.finished $retention_watch 0
tg checkpoint unwatch runner.process.control.retention.finished $retention_watch

touch $trigger_path
let output = job recv --tag $run --timeout 15sec
if $output == null {
	error make { msg: 'the run did not complete' }
}
assert equal $output.exit_code (-15) "the client should be terminated by SIGTERM"

snapshot (server_errors $server) ''
