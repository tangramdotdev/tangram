use ../../lib/test.nu *

# SIGINT waits for running processes and their final index writes in both process modes.

let path = artifact {
	tangram.ts: '
		export default async () => {
			await tg.host.readFile("/target/release");
			return tg.file("late output");
		};
	'
}

# Trace the shutdown, so that the test can observe when the server begins to wait for its runner.
$env.TANGRAM_TRACING = 'info,tangram_server[shutdown]=trace'

for multi_server in [false true] {
	let local = server spawn --config { advanced: { multi_server: $multi_server } }

	# Hold a FIFO open for writing before the process starts, in a holder process that records its pid, so that the process's read blocks until the test kills the holder.
	let directory = mktemp --directory
	let release = $directory | path join release
	let held = $directory | path join held
	mkfifo $release $held
	job spawn {
		bash -c 'exec 3<> "$1"; echo $$ > "$2"; exec sleep 3600' _ $release $held
	} | ignore
	let holder = open --raw $held | str trim | into int

	# Spawn a process that blocks reading the FIFO in its mount, and wait for it to start.
	let id = checkin push $local $path
	let process = tg --url $local.url spawn --sandbox --mount $'($directory):/target' --no-tokens --verbose $id | from json | get process
	wait_for_log $local 'started the process, command='

	# Send SIGINT to the server and wait for its shutdown to reach the runner.
	let pid = open ($local.directory | path join 'lock') | into int
	^kill -INT $pid
	wait_for_log $local 'TRACE http task'

	# The server must keep running while the process runs.
	assert (ps | where pid == $pid | is-not-empty) "the server must wait for the running process"

	# Release the process by killing the holder, which closes the FIFO's last writer. The server must exit once the process finishes.
	^kill -KILL $holder
	wait_for_exit --timeout 30sec $pid "the server must exit after the process finishes"

	# The process must have successfully stored its output after shutdown began.
	let server = server start $local
	let outcome = tg --url $local.url wait $process | from json
	assert equal $outcome.exit 0 "the process must finish successfully during shutdown"
	let output = tg --url $local.url cat $outcome.output.value
	assert equal $output 'late output' "the output must survive shutdown"
	server stop $local
}
