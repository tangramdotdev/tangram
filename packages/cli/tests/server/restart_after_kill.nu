use ../../test.nu *

# A server that is killed while a process is running must be able to start again. It currently hangs before it signals readiness, so the server directory is unusable until the index is deleted.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			console.log("started");
			await tg.sleep(60);
			return tg.file("hello");
		};
	'
}

let build = tg build --detach --verbose $path | from json
let process = $build.process

# Wait until the process is running, so that the server is killed with work in flight.
wait_until { (tg log $process | complete).stdout | str contains 'started' } "the process must start"

# Kill the server.
let pid = open ($server.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_until { ps | where pid == $pid | is-empty } "the server must stop"

# The server must start again and be usable.
spawn --directory $server.directory --url $server.url
let output = tg health | complete
success $output "the server must be usable after being killed"
