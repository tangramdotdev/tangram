use ../../util/test.nu *

# A stopped sandbox keeps its files across a crash and restart of its server and starts again on them, whether it was claimed from the pool or created cold.

for size in [0 1] {
	let server = server spawn --config {
		advanced: { checkpoints: true },
		runner: { sandbox_pool_size: $size },
	}

	# Create a sandbox, claiming it from the pool if the pool has a sandbox.
	let sandbox = if $size == 1 {
		let take = checkpoint watch runner.sandbox.pool.take
		let create = job spawn {
			let id = job id
			tg sandbox create --wait --no-tokens --no-ttl | job send --tag $id 0
		}
		checkpoint pass $take | ignore
		checkpoint unwatch $take
		job recv --tag $create --timeout 60sec | referent node
	} else {
		tg sandbox create --wait --no-tokens --no-ttl | referent node
	}

	# Write a file in the sandbox's first boot, and stop the sandbox.
	let output = tg run --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'echo persisted > /tmp/file' | complete
	success $output 'the write should succeed'
	tg sandbox stop --wait $sandbox

	# Crash the server, and start it again.
	let pid = server_pid $server.directory
	kill --signal 9 $pid
	wait_for_exit $pid
	let server = server start $server

	# The sandbox starts again, and its second boot reads the file.
	success (timeout 60s tg sandbox start --wait $sandbox | complete) 'the sandbox should start'
	let output = tg run --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'read -r line < /tmp/file; echo "$line"' | complete
	success $output 'the read should succeed'
	assert equal ($output.stdout | str trim) persisted
	tg sandbox destroy --wait $sandbox
	server stop $server
}
