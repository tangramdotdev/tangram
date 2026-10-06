use ../lib/test.nu *

# Killing a sandbox while its process is logging must not block indexing.

let local = server spawn --config { advanced: { checkpoints: true } }
let watch = tg checkpoint watch runner.sandbox.pool.take | from json | get watch
let spawn = job spawn {
	let job_id = job id
	let output = tg --url $local.url spawn --sandbox --verbose --executable /bin/sh -- -c 'echo ready; while :; do :; done' | complete
	$output | job send --tag $job_id 0
}

let claimed = timeout 10s tg checkpoint wait runner.sandbox.pool.take $watch 0 | from json
let sandbox_path = $claimed.params.path
tg checkpoint unwatch runner.sandbox.pool.take $watch

let output = job recv --tag $spawn --timeout 10sec
success $output
let process = $output.stdout | from json | get process

wait_until {
	(tg log $process | complete).stdout | str contains 'ready'
} 'the process should produce a log before its sandbox is killed'

let pids = ps --long | where { |row| $row.command | str contains $sandbox_path } | get pid
assert (not ($pids | is-empty)) 'the sandbox should be running'
for pid in $pids {
	kill --signal 9 $pid
}
success (timeout 30s tg wait $process | complete) 'the process should finish after its sandbox is killed'

let output = timeout 30s tg index | complete
success $output 'the index wait should not block on log finalization'

let output = timeout 10s tg log --no-timeout $process | complete
success $output 'the finished log should be readable after its sandbox is killed'
assert ($output.stdout | str contains 'ready') 'the finished log should preserve the received output'
