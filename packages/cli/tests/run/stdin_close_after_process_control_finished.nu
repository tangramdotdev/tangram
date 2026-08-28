use ../../test.nu *

# A stdin close held until the process control task exits observes the finished process instead of retrying forever against the retired control connection.

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
let write_watch = (
	tg checkpoint watch process.stdio.write.request --params '{"close":"true","stream":"stdin"}'
	| from json
	| get watch
)
let destroyed_watch = (
	tg checkpoint watch runner.sandbox.destroyed
	| from json
	| get watch
)
let control_finished_watch = (
	tg checkpoint watch runner.process.control.finished
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

# Hold a stdin close after it has subscribed to process status but before it sends a control request.
let close = job spawn {
	let job_id = job id
	let output = (
		^bash -c '"$1" process stdio write "$2" --stream stdin < /dev/null' _ $tangram $process
		| complete
	)
	$output | job send --tag $job_id 0
}
let output = timeout 10s tg checkpoint wait process.stdio.write.request $write_watch 0 | complete
success $output "the stdin close should reach the server"

# Disconnect the client, destroy the sandbox, and wait until its process control task has exited.
kill --signal 15 $pid
let output = timeout 10s tg checkpoint wait runner.sandbox.destroyed $destroyed_watch 0 | complete
success $output "the sandbox should be destroyed while the stdin close is held"
tg checkpoint continue runner.sandbox.destroyed $destroyed_watch 0
tg checkpoint unwatch runner.sandbox.destroyed $destroyed_watch
let output = timeout 10s tg checkpoint wait runner.process.control.finished $control_finished_watch 0 | complete
success $output "the process control task should finish while the stdin close is held"
tg checkpoint continue runner.process.control.finished $control_finished_watch 0
tg checkpoint unwatch runner.process.control.finished $control_finished_watch

# Release the close after there is no control handler left to acknowledge it.
tg checkpoint continue process.stdio.write.request $write_watch 0
tg checkpoint unwatch process.stdio.write.request $write_watch
let output = try { job recv --tag $close --timeout 10sec } catch { null }
if $output == null {
	error make { msg: 'the stdin close did not complete' }
}
success $output "a stdin close sent after process control exits must return EOF"

touch $trigger_path
let output = try { job recv --tag $run --timeout 15sec } catch { null }
if $output == null {
	error make { msg: 'the run did not complete' }
}
assert equal $output.exit_code (-15) "the client should be terminated by SIGTERM"

snapshot (server_errors $server) ''
