use ../../util/test.nu *

# Signals exit tg run while a reader that never reads blocks its output.

let server = server spawn
let tangram = which tg | where type == external | get path | first
let directory = mktemp -d
let marker = $directory | path join 'marker'
let fifo = $directory | path join 'fifo'
^mkfifo $fifo

# Run a process whose output fills the pipe to a reader that never reads.
let pid_path = $directory | path join 'pid'
let reader_path = $directory | path join 'reader'
let script = ': > marker; while true; do echo output; done'
let run = job spawn {
	let id = job id
	^bash -c 'sleep 60 < "$1" > /dev/null 2>&1 & echo "$!" > "$2"; echo "$$" > "$3"; exec "$4" run --sandbox --mount "$5:$5,rw" --cwd "$5" --executable /bin/sh -- -c "$6" > "$1"' _ $fifo $reader_path $pid_path $tangram $directory $script
	| complete
	| job send --tag $id 0
}
wait_until { $marker | path exists } 'the process should start writing its output'

# Terminate tg run twice, with a pause so that it receives the signals separately, and wait for it to exit.
let pid = open --raw $pid_path | str trim | into int
kill --signal 15 $pid
sleep 1sec
try { kill --signal 15 $pid }
let output = job recv --tag $run --timeout 30sec
assert equal $output.exit_code 143 'tg run should exit with the code for the signal'
kill (open --raw $reader_path | str trim | into int)
