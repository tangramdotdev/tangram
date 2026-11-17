use ../../util/test.nu *

# Every signal to tg run reaches its non-cacheable process unchanged, the process can handle the signals and continue running, and tg run exits with the code of the process.

let server = server spawn
let tangram = which tg | where type == external | get path | first

# Handle each signal without exiting by reporting it through a fifo, then exit with a distinct code when the test permits it.
let script = '
	trap "echo ALRM > signals" ALRM
	trap "echo HUP > signals" HUP
	trap "echo INT > signals" INT
	trap "echo QUIT > signals" QUIT
	trap "echo TERM > signals" TERM
	trap "echo USR1 > signals" USR1
	trap "echo USR2 > signals" USR2
	echo ready > signals
	while [ ! -f finish ]; do :; done
	exit 7
'

# Run the script in a new sandbox that mounts a directory for the fifo and the files.
let directory = mktemp -d
let signals = $directory | path join 'signals'
^mkfifo $signals
let pid_path = mktemp
let run = job spawn {
	let id = job id
	^bash -c 'echo "$$" > "$1"; exec "$2" run --sandbox --mount "$3:$3,rw" --cwd "$3" --executable /bin/sh -- -c "$4"' _ $pid_path $tangram $directory $script
	| complete
	| job send --tag $id 0
}
let output = timeout 30s cat $signals | complete
success $output 'the process should install its signal handlers'
assert equal ($output.stdout | str trim) ready

# Send each signal to tg run, and TERM twice, reading the fifo until the process reports that it handled each one.
let pid = open --raw $pid_path | str trim | into int
for name in [ALRM HUP INT QUIT TERM USR1 USR2 TERM] {
	^kill -s $name $pid
	let output = timeout 30s cat $signals | complete
	success $output $'the process should receive SIG($name)'
	assert equal ($output.stdout | str trim) $name $'the process should receive SIG($name)'
}

# Allow the process to finish.
touch ($directory | path join 'finish')
let output = job recv --tag $run --timeout 30sec
assert equal $output.exit_code 7 'tg run should exit with the code of the process'
