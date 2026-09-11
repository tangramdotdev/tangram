use ../../test.nu *

# SIGINT waits for running processes and their final index writes in both process modes.

let path = artifact {
	tangram.ts: '
		export default async () => {
			console.log("started");
			await tg.sleep(10);
			return tg.file("late output");
		};
	'
}

for single_process in [true false] {
	let server = server spawn --config { advanced: { single_process: $single_process } }

	# Wait until the process is running, so that the server is signaled with work in flight.
	let process = tg --url $server.url build --detach $path | str trim
	wait_until { (tg --url $server.url log $process | complete).stdout | str contains 'started' } "the process must start"

	# Send SIGINT to the server.
	let pid = open ($server.directory | path join 'lock') | into int
	kill --signal 2 $pid

	# The server must keep running while the process runs.
	sleep 2sec
	assert (ps | where pid == $pid | is-not-empty) "the server must wait for the running process"

	# The server must exit once the process finishes.
	wait_until --timeout 30sec { ps | where pid == $pid | is-empty } "the server must exit after the process finishes"

	# The process must have successfully stored its output after shutdown began.
	let server = server start $server
	let outcome = tg --url $server.url wait $process | from json
	assert equal $outcome.exit 0 "the process must finish successfully during shutdown"
	let output = tg --url $server.url cat $outcome.output.value
	assert equal $output 'late output' "the output must survive shutdown"
	server stop $server
}
