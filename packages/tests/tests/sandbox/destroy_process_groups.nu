use ../../util/test.nu *

# Destroying a sandbox stops the processes that its process started in other process groups.
let server = server spawn
let directory = mktemp -d
let marker = $directory | path join 'marker'
let sandbox = tg sandbox create --no-tokens --mount $'($directory):($directory),rw' | referent node
let script = 'set -m; (i=0; while true; do i=$((i+1)); echo $i > marker; done) & wait'
tg spawn --no-tokens $'--sandbox=($sandbox)' --cwd $directory --executable /bin/sh -- -c $script | ignore
let deadline = (date now) + 10sec
while not ($marker | path exists) {
	if (date now) > $deadline {
		error make { msg: 'the loop did not start' }
	}
	sleep 100ms
}
tg sandbox destroy $sandbox
sleep 1sec
let first = open $marker
sleep 2sec
let second = open $marker
assert equal $first $second 'destroying the sandbox should stop every process in it'
