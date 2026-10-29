use ../../util/test.nu *

# SIGTERM shuts the server down immediately, so it does not wait for a running process to finish.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			console.log("started");
			await tg.sleep(60);
		};
	'
}

# Wait until the process is running, so that the server is signaled with work in flight.
let process = tg build --no-tokens --detach $path | referent node
success (timeout 30s tg log --no-timeout --length 8 $process | complete) "the process must start"

# Send SIGTERM to the server.
let pid = open ($local.directory | path join 'lock') | into int
kill --signal 15 $pid

# The server must exit rather than wait for the process to finish.
wait_for_exit --timeout 20sec $pid "the server must exit without waiting for the running process"
