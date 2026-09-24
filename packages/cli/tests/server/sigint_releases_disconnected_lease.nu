use ../lib/test.nu *

# A client disconnect during graceful shutdown must still release its process lease.
let server = server spawn --config {
	advanced: { checkpoints: true },
}
let path = artifact {
	tangram.ts: '
		export default async function () {
			console.log("ready");
			await tg.sleep(300);
		}
	',
}

# Start a build and wait until its wait guard attaches and the process runs.
let watch = tg checkpoint watch process.wait.attach | from json | get watch
let build = job spawn { tg build $path | complete }
let hit = tg checkpoint wait process.wait.attach $watch 0 | from json
tg checkpoint unwatch process.wait.attach $watch
wait_until { (tg log $hit.params.process | complete).stdout | str contains 'ready' } 'the build must start'

# Stop accepting requests while the build client remains connected.
let server_pid = open --raw ($server.directory | path join lock) | str trim | into int
kill --signal 2 $server_pid
wait_until { (timeout 1s tg health | complete).exit_code != 0 } 'the server must stop accepting requests'

# Disconnect the build only after the listener stops, so the wait guard sees a stopped server.
kill --signal 2 (job list | where id == $build | get 0.pids.0)
wait_until { (open --raw $server.exit | str trim) != '' } 'the server must shut down after the build disconnects' --timeout 10sec
assert equal (open --raw $server.exit | str trim | into int) 0 'the server must exit successfully'
