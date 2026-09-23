use ../lib/test.nu *

# Cancelling a build with Ctrl-C and then gracefully stopping the server should not fail its sandbox log reader.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let server = server spawn --config {
	advanced: { checkpoints: true },
	tracing: { stderr_format: 'json' },
}

let watch = tg checkpoint watch process.wait.attach | from json | get watch
let build = job spawn {
	let job_id = job id
	# Keep a subprocess holding the log pipe open after cancellation kills its parent shell. The trailing command prevents a tail exec.
	let output = tg build --executable /bin/sh -a '-c' -a '/bin/sh -c "echo ready; while :; do :; done"; :' | complete
	$output | job send --tag $job_id 0
}

# Wait for output from the running build before sending the first Ctrl-C.
let hit = timeout 10s tg checkpoint wait process.wait.attach $watch 0 | from json
let process = $hit.params.process
tg checkpoint unwatch process.wait.attach $watch
wait_until {
	(tg log $process | complete).stdout | str contains 'ready'
} 'the build should produce a log before cancellation' --timeout 15sec
let pid = job list | where id == $build | get 0.pids.0
kill --signal 2 $pid
let output = job recv --tag $build --timeout 10sec
assert equal $output.exit_code 130 'the build should exit after Ctrl-C'

# A terminal sends Ctrl-C to the foreground process group. Signalling only the server PID misses the sandbox launchers that share its group on Linux.
let pid = open --raw ($server.directory | path join lock) | str trim | into int
assert equal (^ps -o pgid= -p $pid | str trim | into int) $pid
^kill --signal INT -- $'-($pid)'

# Wait for the graceful shutdown to finish before inspecting the complete server log.
wait_until { (open --raw $server.exit | str trim) != '' } 'the server should shut down gracefully' --timeout 30sec
assert equal (open --raw $server.exit | str trim | into int) 0 'the server should exit successfully'
let errors = open --raw $server.log | lines | each { from json } | where level == 'ERROR'
assert ($errors | is-empty) ($errors | to json)
