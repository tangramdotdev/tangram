use ../../util/test.nu *

# A process that is queued in an existing sandbox with a nonzero ttl when its server crashes finishes with the restart's error once the restarted runner stops the sandbox, because it can never start.

let server = server spawn --config { advanced: { checkpoints: true } }

# Create a sandbox, which stops rather than being destroyed when its runner is lost, and spawn a process that keeps it running.
let sandbox = tg sandbox create --wait --no-tokens | referent node
tg spawn --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'while :; do :; done' | ignore

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

# The restarted runner stops the sandbox, and the queued process finishes with the restart's error.
success (timeout 30s tg sandbox status --until stopped $sandbox | complete) 'the sandbox should be stopped'
tg index
let data = tg process get $second | from json
assert equal $data.status finished
assert equal $data.exit 1
assert equal $data.error.node.message 'runner restarted'
