use ../../lib/test.nu *

# A server that is killed while a process is running must be able to start again. It currently hangs before it signals readiness, so the server directory is unusable until the index is deleted.

print -e 'spawning the server'
let local = server spawn

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
success (timeout 30s tg log --no-timeout --length 8 $process | complete) "the process must start"
print -e 'the process is running'

# Kill the server.
let pid = open ($local.directory | path join 'lock') | into int
print -e $'killing the server ($pid)'
kill --signal 9 $pid
wait_for_exit $pid "the server must stop"
let output = ^tangram_tests_supervise wait --directory $local.supervisor --timeout 10 | complete
success $output "the server supervisor must report completion"
assert equal ($output.stdout | str trim | into int) 137 "the supervisor must report the killed server's status"
print -e 'the server stopped'

# The server must start again and be usable.
print -e 'spawning the server again'
let server = server start $local
print -e 'the server started again'
let output = tg health | complete
success $output "the server must be usable after being killed"
print -e 'the server is healthy'
let output = tg index | complete
success $output "awaiting indexing must work after restarting without an indexer ID"

# Repeated stops must observe the persisted completion without waiting for another notification.
server stop $server
assert equal (open --raw $server.exit | str trim | into int) 0 "the supervisor must report a clean shutdown"
server stop $server
