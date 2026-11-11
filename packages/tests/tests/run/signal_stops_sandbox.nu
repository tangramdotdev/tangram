use ../../util/test.nu *

# Interrupting or terminating tg run cancels its process, and tg run exits only after every process in the sandbox that it created has stopped, including a child in another process group.

let server = server spawn
let tangram = which tg | where type == external | get path | first

# Require job control and update the marker without truncating it between writes.
let script = '
	set -em
	case $- in
		*m*) ;;
		*) echo "job control is unavailable" >&2; exit 1 ;;
	esac
	(
		i=0
		while true; do
			i=$((i+1))
			echo $i 1<> marker
		done
	) &
	wait
'

for case in [
	{ exit: 130, signal: 2 },
	{ exit: 143, signal: 15 },
] {
	# Run the script in a new sandbox that mounts a directory for the marker.
	let directory = mktemp -d
	let marker = $directory | path join 'marker'
	let pid_path = mktemp
	let run = job spawn {
		let id = job id
		^bash -c 'echo "$$" > "$1"; exec "$2" run --sandbox --mount "$3:$3,rw" --cwd "$3" --executable /bin/sh -- -c "$4"' _ $pid_path $tangram $directory $script
		| complete
		| job send --tag $id 0
	}
	wait_until { $marker | path exists } 'the child should start updating the marker'

	# Signal tg run and wait for it to exit.
	let pid = open --raw $pid_path | str trim | into int
	kill --signal $case.signal $pid
	let output = job recv --tag $run --timeout 30sec

	# Check that the child no longer changes the marker.
	let previous = open --raw $marker
	sleep 1sec
	assert equal (open --raw $marker) $previous 'tg run should exit after the child in the other process group stops'
	assert equal $output.exit_code $case.exit 'tg run should exit with the code for the signal'
}
