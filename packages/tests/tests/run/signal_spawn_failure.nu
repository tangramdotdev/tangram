use ../../util/test.nu *

# A signal while tg run spawns a sandboxed process terminates tg run when the spawn then fails.

let server = server spawn --config { advanced: { checkpoints: true } }
let tangram = which tg | where type == external | get path | first

# Hold the spawn when its sandbox's control connects, and wait for the runner to create the sandbox, which the runner does concurrently with the connection.
let watch = tg checkpoint watch sandbox.control.connect | from json | get watch
let state_watch = tg checkpoint watch runner.sandbox.state.inserted | from json | get watch
let pid_path = mktemp
let run = job spawn {
	let id = job id
	^bash -c 'echo "$$" > "$1"; exec "$2" run --sandbox --executable /bin/sh -- -c "sleep 60"' _ $pid_path $tangram
	| complete
	| job send --tag $id 0
}
let hit = tg checkpoint wait sandbox.control.connect $watch 0 | from json
tg checkpoint wait runner.sandbox.state.inserted $state_watch 0 | ignore
tg checkpoint continue runner.sandbox.state.inserted $state_watch 0
tg checkpoint unwatch runner.sandbox.state.inserted $state_watch

# Terminate tg run while the spawn is held, then destroy the sandbox and let the spawn fail.
let pid = open --raw $pid_path | str trim | into int
kill --signal 15 $pid
success (tg sandbox destroy $hit.params.sandbox | complete) 'the destroy should succeed'
tg checkpoint continue sandbox.control.connect $watch 0
tg checkpoint unwatch sandbox.control.connect $watch
let output = job recv --tag $run --timeout 30sec
assert equal $output.exit_code (-15) 'tg run should be terminated by the signal'
