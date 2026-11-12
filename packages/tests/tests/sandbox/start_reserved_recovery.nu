use ../../util/test.nu *

# A scheduler that crashes after a start reserved the next boot of a stopped sandbox and before the start enqueued the boot recovers the reserved boot when it starts again, so that the sandbox starts at its next boot with its files without another start.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 2 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", scheduler_ttl: 1, token: $created.token.token },
}
let socket = $remote.url | str replace 'http+unix://' '' | url decode

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

# Start the sandbox, and kill the scheduler's server after the start reserves the next boot and before it enqueues the boot.
let enqueue = checkpoint watch sandbox.start.enqueue --params { sandbox: $sandbox } --token $root_token --url $remote.url
let start = job spawn {
	let id = job id
	try { http post --allow-errors --max-time 30sec --headers { Authorization: $'Bearer ($root_token)' } --unix-socket $socket --content-type application/json $'http://localhost/sandboxes/($sandbox)/start' '{}' } catch { null } | ignore
	0 | job send --tag $id 0
}
checkpoint wait $enqueue | ignore
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
job recv --tag $start --timeout 30sec | ignore

# Start the scheduler's server again. The scheduler recovers the reserved boot, and the sandbox starts.
let remote = server start $remote
mut status = ""
for _ in 0..300 {
	$status = (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status)
	if $status == started { break }
	sleep 100ms
}
assert equal $status started "the recovered boot should start"
let output = tg --url $remote.url --token $root_token run $'--sandbox=($sandbox)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains 'hello') "the file should survive the crash"
tg --url $remote.url --token $root_token sandbox destroy $sandbox
