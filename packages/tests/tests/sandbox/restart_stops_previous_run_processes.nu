use ../../util/test.nu *

# A process of a sandbox that was running when its runner was killed no longer runs after the runner restarts.

let server = server spawn
let directory = mktemp -d
let marker = $directory | path join 'marker'
let sandbox = tg sandbox create --no-tokens --mount $'($directory):($directory),rw' | referent node

# Update the marker in a loop from a child in another process group, without truncating it between writes.
let script = '
	set -em
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

# Wait for the loop to start updating the mounted file.
let output = timeout 10s tg log --no-timeout --length 6 $process | complete
success $output 'the child should write to its log'
let deadline = (date now) + 10sec
while not ($marker | path exists) {
	if (date now) > $deadline {
		error make { msg: 'the loop did not start' }
	}
	sleep 100ms
}

# Kill the runner, then start it again.
let pid = open ($server.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
let server = server start $server

# The loop no longer changes the file.
let previous = open --raw $marker
sleep 1sec
assert equal (open --raw $marker) $previous 'the restarted runner should stop the processes of its previous run'
