use ../../util/test.nu *

# A process that a spawn hands to this server's runner with a new sandbox, whose server crashes before the process starts, finishes with the restart's error, and its sandbox is destroyed, both when the runner created the sandbox's directory and when it did not.

for directory in [true false] {
	let server = server spawn --config { advanced: { checkpoints: true } }

	# Spawn a process with a new sandbox, and hold it once the runner created the sandbox, before the process starts.
	let inserted = checkpoint watch runner.sandbox.state.inserted
	job spawn {
		tg spawn --no-tokens --sandbox --executable /bin/sh -- -c 'exit 0' | complete | ignore
	}
	let sandbox = checkpoint wait $inserted 0 | get params.sandbox
	tg index
	let queued = tg ps --status queued | from json
	assert equal ($queued | length) 1 'the process should be queued'
	let process = $queued | first | get node

	# Crash the server, remove the sandboxes' directories to leave the state of a crash before the runner created the sandbox's directory if requested, and start the server again.
	let pid = open ($server.directory | path join 'lock') | into int
	kill --signal 9 $pid
	wait_for_exit $pid
	if not $directory {
		^chmod -R u+rwx ($server.directory | path join 'sandboxes')
		rm --recursive --force ($server.directory | path join 'sandboxes')
	}
	let server = server start $server

	# The restarted runner destroys the sandbox, and the queued process finishes with the restart's error.
	success (timeout 30s tg sandbox status --until destroyed $sandbox | complete) 'the sandbox should be destroyed'
	tg index
	let data = tg process get $process | from json
	assert equal $data.status finished
	assert equal $data.exit 1
	assert equal $data.error.node.message 'runner restarted'
	server stop $server
}
