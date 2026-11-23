use ../../util/test.nu *

# A process that is queued in an existing sandbox when its server crashes finishes with the restart's error once the restarted runner destroys the sandbox, because it can never start.

let server = server spawn --config { advanced: { checkpoints: true } }

# Spawn a process that keeps a new sandbox started.
let first = tg spawn --no-tokens --verbose --sandbox --executable /bin/sh -- -c 'while :; do :; done' | from json
let sandbox = tg process get $first.process | from json | get sandbox

# Spawn a second process in the sandbox, and hold it before the runner holds it.
let insert = checkpoint watch runner.process.state.insert
job spawn {
	tg spawn --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'exit 0' | complete | ignore
}
let hit = checkpoint wait $insert 0
let second = $hit.params.process
tg index
assert equal (tg process get $second | from json | get status) queued

# Crash the server, and start it again.
let pid = open ($server.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
let server = server start $server

# The restarted runner destroys the sandbox, and the queued process finishes with the restart's error.
success (timeout 30s tg sandbox status --until destroyed $sandbox | complete) 'the sandbox should be destroyed'
tg index
let data = tg process get $second | from json
assert equal $data.status finished
assert equal $data.exit 1
assert equal $data.error.node.message 'runner restarted'
