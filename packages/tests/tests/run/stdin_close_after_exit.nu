use ../../util/test.nu *

# A stdin close that reaches the runner after the process exits, but before the runner observes the exit, succeeds.

let local = server spawn --config { advanced: { checkpoints: true } }

# Hold the runner before it observes the exit.
let exited_watch = tg checkpoint watch runner.process.exited | from json | get watch

# Run a process that exits immediately, holding the client's stdin open with a pipe so that it does not close the process's stdin on its own.
let fifo_directory = mktemp -d
let fifo_path = $fifo_directory | path join 'stdin'
^mkfifo $fifo_path
let trigger_path = $fifo_directory | path join 'close'
let tangram = which tg | where type == external | get path | first
let writer = job spawn {
	^bash -c 'exec 3> "$1"; while [ ! -e "$2" ]; do sleep 0.05; done; exec 3>&-' _ $fifo_path $trigger_path
}
let run = job spawn {
	let job_id = job id
	let output = ^bash -c 'exec "$1" run --sandbox --stdin pipe --executable /bin/sh -- -c "exit 0" < "$2"' _ $tangram $fifo_path | complete
	$output | job send --tag $job_id 0
}
let hit = tg checkpoint wait --timeout 30s runner.process.exited $exited_watch 0 | from json
let process = $hit.params.process

# Close the process's stdin.
let output = ^bash -c 'exec "$1" process stdio write "$2" --stream stdin < /dev/null' _ $tangram $process | complete
success $output "the stdin close should succeed"

# Release the runner and the client's stdin, and the run completes.
tg checkpoint unwatch runner.process.exited $exited_watch
touch $trigger_path
let output = job recv --tag $run --timeout 30sec
success $output "the run should complete"
