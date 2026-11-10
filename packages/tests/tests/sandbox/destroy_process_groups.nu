use ../../util/test.nu *

# Destroying a sandbox finishes promptly and stops a child in another process group that retains the log pipes.

let server = server spawn
let directory = mktemp -d
let marker = $directory | path join 'marker'
let sandbox = tg sandbox create --no-tokens --mount $'($directory):($directory),rw' | referent node

# Require job control and update the marker without truncating it between writes.
let script = '
	set -em
	case $- in
		*m*) ;;
		*) echo "job control is unavailable" >&2; exit 1 ;;
	esac
	(
		echo ready
		i=0
		while true; do
			i=$((i+1))
			echo $i 1<> marker
		done
	) &
	wait
'
let process = tg spawn --no-tokens $'--sandbox=($sandbox)' --cwd $directory --executable /bin/sh -- -c $script | referent node

# Wait for the child to produce output and start updating the mounted file.
let output = timeout 10s tg log --no-timeout --length 6 $process | complete
success $output 'the child should write to its inherited log pipe'
assert equal $output.stdout "ready\n"
let deadline = (date now) + 10sec
while not ($marker | path exists) {
	if (date now) > $deadline {
		error make { msg: 'the loop did not start' }
	}
	sleep 100ms
}

# Await completed destruction before checking that the child no longer changes the file.
success (timeout 10s tg sandbox destroy --wait $sandbox | complete) 'destroying the sandbox should finish while the child holds the log pipes'
let previous = open --raw $marker
sleep 1sec
assert equal (open --raw $marker) $previous 'destroying the sandbox should stop the child in the other process group'

# Keep the output that was captured before the sandbox was destroyed.
let output = timeout 10s tg log $process | complete
success $output 'the process log should remain readable after destruction'
assert equal $output.stdout "ready\n"
