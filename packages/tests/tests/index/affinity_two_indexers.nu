use ../../util/test.nu *

# With two indexers, sandboxes and the processes in them are indexed, and waits for indexing finish, although each sandbox's writes are routed to one indexer.

let root_token = random chars
let instance = instance --cloud --config {
	authentication: { root: { token: $root_token } },
	indexer: { cache: { poll_interval: 0.01 } },
}
let a = server spawn --instance $instance --name a --config { roles: [api indexer scheduler] }
let b = server spawn --instance $instance --name b --config { roles: [api indexer] }
let created = tg --url $a.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $a.url } },
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}

let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.command({
				args: ["-ec", "echo hello"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	',
}
let id = checkin push $a $path --token $root_token
let command = tg --url $a.url --token $root_token build $id | str trim

# Create sandboxes, run a process in each, and stop and destroy them.
for _ in 0..3 {
	let sandbox = tg --url $a.url --token $root_token sandbox create --wait --no-tokens | referent node
	let output = tg --url $a.url --token $root_token run --no-tokens $'--sandbox=($sandbox)' $command | complete
	success $output "the run should succeed"
	assert ($output.stdout | str contains 'hello') "the run should print its output"
	tg --url $a.url --token $root_token sandbox stop --wait $sandbox
	success (tg --url $a.url --token $root_token index | complete) "the wait for indexing should finish"
	assert equal (tg --url $b.url --token $root_token sandbox get $sandbox | from json | get data.status) stopped
	success (timeout 30s tg --url $a.url --token $root_token sandbox destroy --wait $sandbox | complete) "the destroy should finish"
	success (tg --url $b.url --token $root_token index | complete) "the wait for indexing should finish"
	assert equal (tg --url $b.url --token $root_token sandbox get $sandbox | from json | get data.status) destroyed
}
