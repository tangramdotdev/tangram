use ../../util/test.nu *

# A remote runner's sandbox that was running when the runner was killed is stopped after the runner restarts, and it starts again with its files.

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

let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens | referent node
success (tg --url $remote.url --token $root_token run $'--sandbox=($sandbox)' $write | complete) "the write should succeed"

# Kill the runner, then start it again and wait for it to stop the sandbox.
let stopped = checkpoint watch runner.control.stop_sandbox --params { sandbox: $sandbox } --token $root_token --url $remote.url
let runner_pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $runner_pid
wait_for_exit $runner_pid
let runner = server start $runner
checkpoint pass $stopped
checkpoint unwatch $stopped
tg --url $remote.url --token $root_token index

assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) stopped "the sandbox should be stopped after the runner restarts"
tg --url $remote.url --token $root_token sandbox start $sandbox
let output = tg --url $remote.url --token $root_token run $'--sandbox=($sandbox)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains "hello") "the file should survive the restart"
