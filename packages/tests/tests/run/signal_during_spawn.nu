use ../../util/test.nu *

# A signal while tg run spawns a sandboxed process stops the process once the spawn finishes, before tg run exits.

let server = server spawn --config { advanced: { checkpoints: true } }
let tangram = which tg | where type == external | get path | first
let directory = mktemp -d
let marker = $directory | path join 'marker'

# Hold the spawn when its sandbox's control connects.
let watch = tg checkpoint watch sandbox.control.connect | from json | get watch
let pid_path = mktemp
let script = 'i=0; while true; do i=$((i+1)); echo $i 1<> marker; done'
let run = job spawn {
	let id = job id
	^bash -c 'echo "$$" > "$1"; exec "$2" run --sandbox --mount "$3:$3,rw" --cwd "$3" --executable /bin/sh -- -c "$4"' _ $pid_path $tangram $directory $script
	| complete
	| job send --tag $id 0
}
tg checkpoint wait sandbox.control.connect $watch 0 | ignore

# Terminate tg run while the spawn is held, then let the spawn finish.
let pid = open --raw $pid_path | str trim | into int
kill --signal 15 $pid
tg checkpoint continue sandbox.control.connect $watch 0
tg checkpoint unwatch sandbox.control.connect $watch
let output = job recv --tag $run --timeout 30sec
assert equal $output.exit_code 143 'tg run should exit with the code for the signal'

# Check that the process does not run after tg run exits.
let previous = if ($marker | path exists) { open --raw $marker } else { '' }
sleep 1sec
let current = if ($marker | path exists) { open --raw $marker } else { '' }
assert equal $current $previous 'the process should not run after tg run exits'
