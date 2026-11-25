use ../../util/test.nu *

# A sandbox get issued after its runner dies must observe the end of the sandbox's boot without waiting for the control read timeout, and the sandbox is then destroyed.
let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	control: { read_timeout: 60 },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { cpus: 1, overcommit: { cpu: 1 }, id: $created.data.id, remote: "default", token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(120);
		}
	',
}
let id = tg --url $local.url build --wait --no-tokens --remote --detach $path | referent node
let sandbox = tg --url $remote.url --token $root_token get $id | from json | get sandbox
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) "started"

let runner_pid = server_pid $runner.directory
kill --signal 9 $runner_pid
wait_for_exit $runner_pid

# Wait for the get to read the index and wait on the runner's control, then advance the remote's clock past the runner ttl so that the scheduler expires the runner.
let index = checkpoint watch sandbox.get.index --params { sandbox: $sandbox } --token $root_token --url $remote.url
let get = job spawn {
	let job_id = job id
	tg --url $remote.url --token $root_token sandbox get $sandbox | complete | job send --tag $job_id 0
}
checkpoint pass $index | ignore
checkpoint unwatch $index
advance_time $remote 4sec
let output = job recv --tag $get --timeout 15sec
success $output "the sandbox get must complete before the control read timeout"
assert (($output.stdout | from json | get data.status) in [stopped destroyed]) "the pending get must observe the end of the boot once the runner expires"

# The sandbox, whose zero ttl lets it not stay stopped, is then destroyed.
success (timeout 30s tg --url $remote.url --token $root_token sandbox status --until destroyed $sandbox | complete) "the sandbox should be destroyed"

# Waiting for the sandbox returns the error that destroyed it.
let output = tg --url $remote.url --token $root_token sandbox wait $sandbox | complete
success $output
snapshot $output.stdout '
	{"error":{"code":"internal","message":"heartbeat expired"}}

'
