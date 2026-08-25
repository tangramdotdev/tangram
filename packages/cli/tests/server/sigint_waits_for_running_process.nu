use ../../test.nu *

# SIGINT shuts the server down gracefully, so it waits for a running process to finish before it exits.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			console.log("started");
			await tg.sleep(10);
		};
	'
}

# Wait until the process is running, so that the server is signaled with work in flight.
let process = tg build --detach $path | str trim
wait_until { (tg log $process | complete).stdout | str contains 'started' } "the process must start"

# Send SIGINT to the server.
let pid = open ($server.directory | path join 'lock') | into int
kill --signal 2 $pid

# The server must keep running while the process runs.
sleep 2sec
assert (ps | where pid == $pid | is-not-empty) "the server must wait for the running process"

# The server must exit once the process finishes.
wait_until --timeout 30sec { ps | where pid == $pid | is-empty } "the server must exit after the process finishes"
