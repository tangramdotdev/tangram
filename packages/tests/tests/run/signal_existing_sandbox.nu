use ../../util/test.nu *

# A signal to tg run reaches its process in an existing sandbox, tg run exits with the code of the process, and the sandbox remains usable.

let server = server spawn
let tangram = which tg | where type == external | get path | first
let directory = mktemp -d
let marker = $directory | path join 'marker'
let sandbox = tg sandbox create --no-tokens --mount $'($directory):($directory),rw' | referent node

# Run a process that handles the signal before exiting.
let pid_path = mktemp
let script = 'trap "echo TERM > marker; exit 7" TERM; echo ready > marker; while true; do :; done'
let run = job spawn {
	let id = job id
	^bash -c 'echo "$$" > "$1"; exec "$2" run --no-tokens "--sandbox=$3" --cwd "$4" --executable /bin/sh -- -c "$5"' _ $pid_path $tangram $sandbox $directory $script
	| complete
	| job send --tag $id 0
}
wait_until { $marker | path exists } 'the process should install its signal handler'

# Terminate tg run and wait for it to exit.
let pid = open --raw $pid_path | str trim | into int
kill --signal 15 $pid
let output = job recv --tag $run --timeout 30sec

# Check that the process handled the signal and another process can use the sandbox.
assert equal (open --raw $marker | str trim) TERM 'the process should receive SIGTERM'
assert equal $output.exit_code 7 'tg run should exit with the code of the process'
assert equal (tg run --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'echo usable' | str trim) usable 'the sandbox should remain usable'
tg sandbox destroy $sandbox
