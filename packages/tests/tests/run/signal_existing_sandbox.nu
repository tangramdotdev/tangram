use ../../util/test.nu *

# A signal that ends tg run cancels its process in an existing sandbox, and leaves the sandbox running.

let server = server spawn
let tangram = which tg | where type == external | get path | first
let directory = mktemp -d
let marker = $directory | path join 'marker'
let sandbox = tg sandbox create --no-tokens --mount $'($directory):($directory),rw' | referent node

# Run a process that updates the marker in the sandbox.
let pid_path = mktemp
let script = 'i=0; while true; do i=$((i+1)); echo $i 1<> marker; done'
let run = job spawn {
	let id = job id
	^bash -c 'echo "$$" > "$1"; exec "$2" run --no-tokens "--sandbox=$3" --cwd "$4" --executable /bin/sh -- -c "$5"' _ $pid_path $tangram $sandbox $directory $script
	| complete
	| job send --tag $id 0
}
wait_until { $marker | path exists } 'the process should start updating the marker'

# Terminate tg run and wait for it to exit.
let pid = open --raw $pid_path | str trim | into int
kill --signal 15 $pid
let output = job recv --tag $run --timeout 30sec

# Check that the process stopped and that the sandbox is still running.
let previous = open --raw $marker
sleep 1sec
assert equal (open --raw $marker) $previous 'tg run should exit after the process stops'
assert equal $output.exit_code 143 'tg run should exit with the code for the signal'
assert equal (tg sandbox get $sandbox | from json | get data.status) started 'the sandbox should keep running'
tg sandbox destroy $sandbox
