use ../../util/test.nu *

# A signal to tg run reaches its non-cacheable process unchanged, and the process can handle it and continue running until it chooses to exit.

let server = server spawn
let tangram = which tg | where type == external | get path | first

# Handle each signal without exiting, then finish when the test permits it.
let script = '
	trap "echo HUP > signal" HUP
	trap "echo INT > signal" INT
	trap "echo TERM > signal" TERM
	echo ready > marker
	while [ ! -f finish ]; do :; done
	echo finished > marker
'

for case in [
	{ exit: 129, name: HUP, signal: 1 },
	{ exit: 130, name: INT, signal: 2 },
	{ exit: 143, name: TERM, signal: 15 },
] {
	# Run the script in a new sandbox that mounts a directory for the marker.
	let directory = mktemp -d
	let marker = $directory | path join 'marker'
	let received = $directory | path join 'signal'
	let pid_path = mktemp
	let run = job spawn {
		let id = job id
		^bash -c 'echo "$$" > "$1"; exec "$2" run --sandbox --mount "$3:$3,rw" --cwd "$3" --executable /bin/sh -- -c "$4"' _ $pid_path $tangram $directory $script
		| complete
		| job send --tag $id 0
	}
	wait_until { $marker | path exists } 'the process should install its signal handlers'

	# Signal tg run and wait for the process to handle the original signal.
	let pid = open --raw $pid_path | str trim | into int
	kill --signal $case.signal $pid
	wait_until { try { (open --raw $received | str trim) == $case.name } catch { false } } 'the process should receive the original signal'

	# Allow the process to finish after its signal handler returns.
	touch ($directory | path join 'finish')
	let output = job recv --tag $run --timeout 30sec
	assert equal (open --raw $marker | str trim) finished 'the process should keep running after handling the signal'
	assert equal $output.exit_code $case.exit 'tg run should exit with the code for the signal'
}
