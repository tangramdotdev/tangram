use ../../lib/test.nu *

# Cancelling a build with Ctrl-C and then gracefully stopping the server with Ctrl-C exits the server successfully.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let local = server spawn --config {
	advanced: { checkpoints: true },
}

let watch = tg checkpoint watch process.wait.attach | from json | get watch
let build = job spawn {
	let job_id = job id
	# Keep a subprocess holding the log pipe open after cancellation kills its parent shell. The trailing command prevents a tail exec.
	let output = tg build --executable /bin/sh -a '-c' -a '/bin/sh -c "echo ready; while :; do :; done"; :' | complete
	$output | job send --tag $job_id 0
}

# Wait for output from the running build before sending the first Ctrl-C.
let hit = tg checkpoint wait --timeout 10s process.wait.attach $watch 0 | from json
let process = $hit.params.process
tg checkpoint unwatch process.wait.attach $watch
let log = timeout 15s tg log --no-timeout --stream stdout --length 6 $process | complete
assert equal $log.stdout "ready\n" 'the build should produce a log before cancellation'
let pid = job list | where id == $build | get 0.pids.0
kill --signal 2 $pid
let output = job recv --tag $build --timeout 10sec
assert equal $output.exit_code 130 'the build should exit after Ctrl-C'

# A terminal sends Ctrl-C to the foreground process group. Signalling only the server PID misses the sandbox launchers that share its group on Linux.
let pid = open --raw ($local.directory | path join lock) | str trim | into int
^kill --signal INT -- $'-($pid)'

# Wait for the graceful shutdown to finish.
let exit = ^tangram_tests_supervise wait --directory $local.supervisor --timeout 30 | str trim | into int
assert equal $exit 0 'the server should exit successfully'
