use ../../util/test.nu *

# A tty size request whose runner dies after acknowledging the request but before responding must return rather than hang forever.
let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	control: { read_timeout: 60 },
	roles: [api indexer scheduler],
	scheduler: { runner_time_to_live: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}

# Spawn a long-running, non-cacheable process with a tty on the runner.
let path = artifact {
	tangram.ts: 'export default async () => { await tg.sleep(120); };',
}
let spawned = tg --url $local.url spawn --remote --network --tty=24,80 --verbose $path | from json
let process = $spawned.process

# Hold the runner after it receives and acknowledges the tty size request but before it responds.
let tty_watch = tg --url $runner.url checkpoint watch runner.process.control.tty.received | from json | get watch
let local_socket = $local.url | str replace 'http+unix://' '' | url decode
let body = { size: { cols: 100, rows: 30 }, tokens: $spawned.tokens } | to json --raw
let request = job spawn {
	let job_id = job id
	let status = (
		$body | into binary | http put --allow-errors --headers { 'Content-Type': 'application/json' }
			--max-time 120sec --unix-socket $local_socket $'http://localhost/processes/($process)/tty/size'
		| metadata | get http_response.status
	)
	$status | job send --tag $job_id 0
}
success (tg --url $runner.url checkpoint wait --timeout 30s runner.process.control.tty.received $tty_watch 0 | complete) "the runner must receive the tty size request"

# Kill the runner while it holds the acknowledged tty size request, so that it never sends a response.
let runner_pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $runner_pid
wait_for_exit $runner_pid

# Advance the remote's clock past the runner time_to_live so that the scheduler expires the dead runner.
advance_time $remote 4sec

# The tty size request must return rather than hang waiting for the response from the dead runner.
let status = try { job recv --tag $request --timeout 30sec } catch { null }
assert ($status != null) "the tty size request must return after the runner dies before responding"
assert equal $status 200
