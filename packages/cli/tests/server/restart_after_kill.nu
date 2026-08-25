use ../../test.nu *

# A server that is killed while a process is running must be able to start again. It currently hangs before it signals readiness, so the server directory is unusable until the index is deleted.

print -e 'spawning the server'
let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			console.log("started");
			await tg.sleep(60);
			return tg.file("hello");
		};
	'
}

print -e 'building'
let build = tg build --detach --verbose $path | from json
let process = $build.process
print -e $'built ($process)'

# Wait until the process is running, so that the server is killed with work in flight.
wait_until { (tg log $process | complete).stdout | str contains 'started' } "the process must start"
print -e 'the process is running'

# Kill the server.
let pid = open ($server.directory | path join 'lock') | into int
print -e $'killing the server ($pid)'
kill --signal 9 $pid
wait_until { ps | where pid == $pid | is-empty } "the server must stop"
print -e 'the server stopped'

# The server must start again and be usable.
print -e 'spawning the server again'
let server = server start $server
print -e 'the server started again'
let output = tg health | complete
success $output "the server must be usable after being killed"
print -e 'the server is healthy'
