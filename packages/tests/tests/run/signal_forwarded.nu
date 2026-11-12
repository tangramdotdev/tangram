use ../../util/test.nu *

# Every signal to tg run reaches its non-cacheable process unchanged, the process can handle the signals and continue running, and tg run exits with the code of the process.

let server = server spawn
let tangram = which tg | where type == external | get path | first

# Handle each signal without exiting, then exit with a distinct code when the test permits it.
let script = '
	trap "echo ALRM >> signals" ALRM
	trap "echo HUP >> signals" HUP
	trap "echo INT >> signals" INT
	trap "echo QUIT >> signals" QUIT
	trap "echo TERM >> signals" TERM
	trap "echo USR1 >> signals" USR1
	trap "echo USR2 >> signals" USR2
	echo ready > marker
	while [ ! -f finish ]; do :; done
	exit 7
'

# Run the script in a new sandbox that mounts a directory for the files.
let directory = mktemp -d
let marker = $directory | path join 'marker'
let signals = $directory | path join 'signals'
let pid_path = mktemp
let run = job spawn {
	let id = job id
	^bash -c 'echo "$$" > "$1"; exec "$2" run --sandbox --mount "$3:$3,rw" --cwd "$3" --executable /bin/sh -- -c "$4"' _ $pid_path $tangram $directory $script
	| complete
	| job send --tag $id 0
}
wait_until { $marker | path exists } 'the process should install its signal handlers'

# Send each signal to tg run, and TERM twice, waiting for the process to handle each one.
let pid = open --raw $pid_path | str trim | into int
let names = [ALRM HUP INT QUIT TERM USR1 USR2 TERM]
for entry in ($names | enumerate) {
	^kill -s $entry.item $pid
	let expected = $names | take ($entry.index + 1)
	wait_until { try { (open --raw $signals | lines) == $expected } catch { false } } $'the process should receive SIG($entry.item)'
}

# Allow the process to finish.
touch ($directory | path join 'finish')
let output = job recv --tag $run --timeout 30sec
assert equal $output.exit_code 7 'tg run should exit with the code of the process'
