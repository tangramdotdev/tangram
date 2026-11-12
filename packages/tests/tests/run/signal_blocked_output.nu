use ../../util/test.nu *

# A signal after the process finishes terminates tg run while a reader that never reads blocks its output, whether or not the output ends with a newline.

let server = server spawn --config { advanced: { checkpoints: true } }
let tangram = which tg | where type == external | get path | first

for entry in (['echo output' 'printf output'] | enumerate) {
	let directory = mktemp -d
	let fifo = $directory | path join 'fifo'
	^mkfifo $fifo

	# Run a process whose output fills the pipe to a reader that never reads.
	let watch = tg checkpoint watch process.wait.attach | from json | get watch
	let pid_path = $directory | path join 'pid'
	let reader_path = $directory | path join 'reader'
	let script = $'while true; do ($entry.item); done'
	let run = job spawn {
		let id = job id
		^bash -c 'sleep 60 < "$1" > /dev/null 2>&1 & echo "$!" > "$2"; echo "$$" > "$3"; exec "$4" run --sandbox --executable /bin/sh -- -c "$5" > "$1"' _ $fifo $reader_path $pid_path $tangram $script
		| complete
		| job send --tag $id 0
	}
	let hit = tg checkpoint wait process.wait.attach $watch 0 | from json
	tg checkpoint continue process.wait.attach $watch 0
	tg checkpoint unwatch process.wait.attach $watch
	let process = $hit.params.process

	# Terminate tg run, which forwards the signal, and wait for the process to finish.
	let pid = open --raw $pid_path | str trim | into int
	kill --signal 15 $pid
	let outcome = tg wait $process | from json
	assert equal $outcome.exit 143 'the process should be terminated by the forwarded signal'

	# Terminate tg run until it exits, because it forwards a signal that arrives before it learns that the process finished.
	mut result: any = null
	for _ in 0..300 {
		try { kill --signal 15 $pid }
		$result = try { job recv --tag $run --timeout 100ms } catch { null }
		if $result != null {
			break
		}
	}
	assert ($result != null) 'tg run should exit'
	assert ($result.exit_code in [-15 143]) 'tg run should be terminated by the signal or exit with the code of the process'
	kill (open --raw $reader_path | str trim | into int)
}
