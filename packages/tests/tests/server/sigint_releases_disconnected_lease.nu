use ../../lib/test.nu *

# A client disconnect during graceful shutdown must still release its process lease.
let local = server spawn --name local --config {
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
success (timeout 30s tg log --no-timeout --length 6 $hit.params.process | complete) 'the build must start'

# Stop accepting requests while the build client remains connected.
let server_pid = open --raw ($local.directory | path join lock) | str trim | into int
kill --signal 2 $server_pid
wait_for_log $local 'stopped listening on'

# Disconnect the build only after the listener stops, so the wait guard sees a stopped server.
kill --signal 2 (job list | where id == $build | get 0.pids.0)
let output = ^tangram_tests_supervisor wait --directory $local.supervisor --timeout 10 | complete
success $output 'the server must shut down after the build disconnects'
assert equal ($output.stdout | str trim | into int) 0 'the server must exit successfully'
