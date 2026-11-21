use ../../util/test.nu *

# A spawn that the sandbox's scheduler sends to the runner of a boot that ended before the spawn arrived fails, and does not run in the sandbox's next boot.

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
let url = $remote.url

let path = artifact {
	tangram.ts: '
		export function hello() {
			return tg.command({
				args: ["-ec", "echo hello"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	',
}
let id = checkin push $remote $path --token $root_token
let command = tg --url $url --token $root_token build $'($id)#hello' | str trim
let sandbox = tg --url $url --token $root_token sandbox create --wait --no-tokens | referent node

# Spawn into the sandbox, and hold the scheduler before it sends the spawn to the sandbox's runner.
let spawn = checkpoint watch scheduler.process.spawn --params { sandbox: $sandbox } --token $root_token --url $url
let job = job spawn {
	let id = job id
	timeout 60s tg --url $url --token $root_token process spawn --no-tokens $'--sandbox=($sandbox)' $command | complete | job send --tag $id 0
}
checkpoint wait $spawn | ignore

# Stop the sandbox and start its next boot.
success (timeout 30s tg --url $url --token $root_token sandbox stop --wait $sandbox | complete) "the stop should succeed"
success (timeout 30s tg --url $url --token $root_token sandbox start --wait $sandbox | complete) "the start should succeed"

# Let the scheduler send the spawn, which is for the boot that ended. The spawn fails, and the runner refuses the spawn for the boot that it is not running.
checkpoint unwatch $spawn
let output = job recv --tag $job --timeout 60sec
failure $output "the spawn for the ended boot should fail"
let data = tg --url $url --token $root_token sandbox get $sandbox | from json | get data
assert equal $data.status started "the sandbox should be started"
assert equal $data.boot 1 "the sandbox should be at its next boot"
