use ../../test.nu *

# A server that receives SIGINT while a process is running must finish its graceful shutdown. It currently hangs, because the shutdown awaits the runner task, and the runner waits for every running sandbox to finish without stopping the processes in it. The abort of the sandbox tasks that would end the wait is sequenced after it, so it is never reached.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			console.log("started");
			await tg.sleep(60);
		};
	'
}

# Wait until the process is running, so that the server is interrupted with work in flight.
let process = tg build --detach $path | str trim
wait_until { (tg log $process | complete).stdout | str contains 'started' } "the process must start"

# Send SIGINT to the server, as a terminal interrupt does.
let pid = open ($server.directory | path join 'lock') | into int
kill --signal 2 $pid

# The server must exit rather than wait for the process to finish.
wait_until --timeout 20sec { ps | where pid == $pid | is-empty } "the server must finish its graceful shutdown"
