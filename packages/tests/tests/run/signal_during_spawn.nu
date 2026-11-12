use ../../util/test.nu *

# A signal while tg run spawns a sandboxed process is forwarded once the spawn finishes, and tg run exits with the code of the process.

let server = server spawn --config { advanced: { checkpoints: true } }
let tangram = which tg | where type == external | get path | first

# Hold the spawn when its sandbox's control connects.
let watch = tg checkpoint watch sandbox.control.connect | from json | get watch
let pid_path = mktemp
let run = job spawn {
	let id = job id
	^bash -c 'echo "$$" > "$1"; exec "$2" run --sandbox --executable /bin/sh -- -c "while true; do :; done"' _ $pid_path $tangram
	| complete
	| job send --tag $id 0
}
tg checkpoint wait sandbox.control.connect $watch 0 | ignore

# Terminate tg run while the spawn is held, then let the spawn finish.
let pid = open --raw $pid_path | str trim | into int
kill --signal 15 $pid
tg checkpoint continue sandbox.control.connect $watch 0
tg checkpoint unwatch sandbox.control.connect $watch

# The process never exits by itself, so tg run exits only if the process receives the signal.
let output = job recv --tag $run --timeout 30sec
assert equal $output.exit_code 143 'tg run should exit with the code of the process, which the signal terminated'
