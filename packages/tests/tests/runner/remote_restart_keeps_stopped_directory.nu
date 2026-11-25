use ../../util/test.nu *

# A remote runner that restarts keeps the directory of a sandbox that the remote stops, so that it starts again with its files, and loses a sandbox that the remote destroys.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

let path = artifact {
	tangram.ts: '
		export function write() {
			return tg.command({
				args: ["-ec", "echo hello > /tmp/file"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
		export function read() {
			return tg.command({
				args: ["-ec", "read -r line < /tmp/file; echo \"$line\""],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	',
}
let id = checkin push $remote $path --token $root_token
let write = tg --url $remote.url --token $root_token build $'($id)#write' | str trim
let read = tg --url $remote.url --token $root_token build $'($id)#read' | str trim

# Create a sandbox that stops when its runner is lost and write a file in it, and a sandbox with a zero ttl, which is destroyed when its runner is lost.
let stopped = tg --url $remote.url --token $root_token sandbox create --no-tokens | referent node
success (tg --url $remote.url --token $root_token run $'--sandbox=($stopped)' $write | complete) "the write should succeed"
let destroyed = tg --url $remote.url --token $root_token sandbox create --wait --no-tokens --time-to-live 0 | referent node

# Kill the runner, and start it again.
let runner_pid = server_pid $runner.directory
kill --signal 9 $runner_pid
wait_for_exit $runner_pid
let runner = server start $runner

# The remote stops the first sandbox and destroys the second.
success (timeout 30s tg --url $remote.url --token $root_token sandbox status --until stopped $stopped | complete) "the first sandbox should stop"
success (timeout 30s tg --url $remote.url --token $root_token sandbox status --until destroyed $destroyed | complete) "the second sandbox should be destroyed"
let data = tg --url $remote.url --token $root_token sandbox get $destroyed | from json | get data
assert equal $data.status destroyed
assert equal $data.error.message 'runner restarted'

# The stopped sandbox starts again on the restarted runner with its files.
success (timeout 60s tg --url $remote.url --token $root_token sandbox start --wait $stopped | complete) "the stopped sandbox should start"
let output = tg --url $remote.url --token $root_token run $'--sandbox=($stopped)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains "hello") "the file should survive the restart"
