use ../../test.nu *

# SIGTERM shuts the server down immediately, so it does not wait for a running process to finish.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			console.log("started");
			await tg.sleep(60);
		};
	'
}

# Wait until the process is running, so that the server is signaled with work in flight.
let process = tg build --detach $path | str trim
wait_until { (tg log $process | complete).stdout | str contains 'started' } "the process must start"

# Send SIGTERM to the server.
let pid = open ($server.directory | path join 'lock') | into int
kill --signal 15 $pid

# The server must exit rather than wait for the process to finish.
wait_until --timeout 20sec { ps | where pid == $pid | is-empty } "the server must exit without waiting for the running process"
