use ../../util/test.nu *

# A start of a stopped sandbox whose runner restarts after the scheduler reserves the next boot and before the scheduler holds the boot starts the sandbox on the restarted runner with its files.

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

# Create a sandbox with a file, and stop it.
let sandbox = tg --url $remote.url --token $root_token sandbox create --wait --no-tokens | referent node
success (tg --url $remote.url --token $root_token run $'--sandbox=($sandbox)' $write | complete) "the write should succeed"
tg --url $remote.url --token $root_token sandbox stop --wait $sandbox

# Start the sandbox, and hold the scheduler after it reserves the next boot and before it holds the boot.
let enqueue = checkpoint watch scheduler.sandbox.reserved --params { sandbox: $sandbox } --token $root_token --url $remote.url
let url = $remote.url
let start = job spawn {
	let id = job id
	timeout 60s tg --url $url --token $root_token sandbox start --wait $sandbox | complete | job send --tag $id 0
}
checkpoint wait $enqueue | ignore

# Crash the runner, start it again, and wait for the scheduler to finish its connection.
let added = checkpoint watch scheduler.runner.added --params { runner: $created.data.id } --token $root_token --url $remote.url
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
let runner = server start $runner
checkpoint wait $added | ignore
checkpoint unwatch $added

# Let the start enqueue the boot. The sandbox starts on the restarted runner with its file.
checkpoint unwatch $enqueue
let output = job recv --tag $start --timeout 60sec
success $output "the start should succeed after the runner restarts"
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) started "the sandbox should be started"
let output = tg --url $remote.url --token $root_token run $'--sandbox=($sandbox)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains 'hello') "the file should survive the restart"
tg --url $remote.url --token $root_token sandbox destroy $sandbox
