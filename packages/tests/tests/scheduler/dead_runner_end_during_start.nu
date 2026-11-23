use ../../util/test.nu *

# A start of a stopped sandbox that is reserved while the first connection of its restarted runner ends the runner's boots is left to the scheduler, so that the sandbox starts with its files.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 300 },
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

# Create a sandbox, write a file in it, and stop it.
let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens | referent node
success (tg --url $remote.url --token $root_token run $'--sandbox=($sandbox)' $write | complete) "the write should succeed"
tg --url $remote.url --token $root_token sandbox stop --wait $sandbox

# Kill the runner, start it again, and hold its connection once it has read the sandboxes that the index records on it.
let restarted = checkpoint watch scheduler.runner.restarted --params { runner: $created.data.id } --token $root_token --url $remote.url
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid "the runner must exit"
let runner = server start $runner
checkpoint wait $restarted 0 | ignore

# Start the sandbox while the connection is held, which reserves its next boot.
success (timeout 30s tg --url $remote.url --token $root_token sandbox start $sandbox | complete) "the start should be accepted"

# Let the connection continue. The sandbox starts on the restarted runner with its files.
checkpoint pass $restarted
checkpoint unwatch $restarted
let output = timeout 60s tg --url $remote.url --token $root_token run $'--sandbox=($sandbox)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains "hello") "the file should survive the restart"
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) started
