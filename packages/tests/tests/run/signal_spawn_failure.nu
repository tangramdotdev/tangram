use ../../util/test.nu *

# A signal while tg run spawns a sandboxed process exits with the code for the signal when the spawn then fails.

let server = server spawn --config { advanced: { checkpoints: true } }
let tangram = which tg | where type == external | get path | first

# Hold the spawn when its sandbox's control connects.
let watch = tg checkpoint watch sandbox.control.connect | from json | get watch
let pid_path = mktemp
let run = job spawn {
	let id = job id
	^bash -c 'echo "$$" > "$1"; exec "$2" run --sandbox --executable /bin/sh -- -c "sleep 60"' _ $pid_path $tangram
	| complete
	| job send --tag $id 0
}
let hit = tg checkpoint wait sandbox.control.connect $watch 0 | from json

# Terminate tg run while the spawn is held, then destroy the sandbox and let the spawn fail.
let pid = open --raw $pid_path | str trim | into int
kill --signal 15 $pid
wait_until { (tg sandbox destroy $hit.params.sandbox | complete | get exit_code) == 0 } 'the destroy should succeed'
tg checkpoint continue sandbox.control.connect $watch 0
tg checkpoint unwatch sandbox.control.connect $watch
let output = job recv --tag $run --timeout 30sec
assert equal $output.exit_code 143 'tg run should exit with the code for the signal'
