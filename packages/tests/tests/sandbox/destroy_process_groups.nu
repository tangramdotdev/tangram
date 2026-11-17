use ../../util/test.nu *

# Destroying a sandbox finishes promptly and stops a child in another process group that retains the log pipes.

let server = server spawn
let directory = mktemp -d
let alive = $directory | path join 'alive'
^mkfifo $alive
let sandbox = tg sandbox create --no-tokens --mount $'($directory):($directory),rw' | referent node

# Require job control, and run a child in another process group that holds the write end of the fifo for as long as it lives.
let script = '
	set -em
	case $- in
		*m*) ;;
		*) echo "job control is unavailable" >&2; exit 1 ;;
	esac
	(
		exec 3> alive
		echo ready
		while true; do :; done
	) &
	wait
'

# Read the fifo until every process that holds its write end exits.
let reader = job spawn {
	let id = job id
	timeout 30s cat $alive | complete | job send --tag $id 0
}
let process = tg spawn --no-tokens $'--sandbox=($sandbox)' --cwd $directory --executable /bin/sh -- -c $script | referent node

# Wait for the child to open the fifo and produce output.
let output = timeout 10s tg log --no-timeout --length 6 $process | complete
success $output 'the child should write to its inherited log pipe'
assert equal $output.stdout "ready\n"

# Await completed destruction, and check that the child in the other process group exits, which closes the fifo.
success (timeout 10s tg sandbox destroy --wait $sandbox | complete) 'destroying the sandbox should finish while the child holds the log pipes'
success (job recv --tag $reader --timeout 10sec) 'destroying the sandbox should stop the child in the other process group'

# Keep the output that was captured before the sandbox was destroyed.
let output = timeout 10s tg log $process | complete
success $output 'the process log should remain readable after destruction'
assert equal $output.stdout "ready\n"
