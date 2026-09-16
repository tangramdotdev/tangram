use ../../test.nu *

# Local sandbox operations bypass owner-side control dispatch without borrowing the runner's authority.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let watch = tg --url $remote.url --token $root_token checkpoint watch sandbox.control.request --params '{"kind":"spawn_process"}' | from json | get watch
let path = artifact {
	tangram.ts: '
		export default async function () {
			const child = await tg.spawn`printf local`.stdio("pipe");
			const [text, wait] = await Promise.all([child.stdout.text(), child.wait()]);
			tg.assert(text === "local" && wait.exit === 0);
			return "ok";
		}
	',
}
let output = timeout 30s tg --url $remote.url --token $root_token build $path | complete
let hit = timeout 1s tg --url $remote.url --token $root_token checkpoint wait sandbox.control.request $watch 0 | complete
tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.request $watch
success $output
assert equal ($output.stdout | from json) "ok"
assert equal $hit.exit_code 124 "spawning in the current sandbox must not contact owner-side control"

let sandbox = tg --url $remote.url --token $root_token sandbox create | str trim
let reader = tg --url $runner.url login --verbose --name reader | from json
tg --url $runner.url --token $reader.token remote put default $remote.url
tg --url $runner.url --token $root_token grant $reader.user.id read $sandbox
let socket = $runner.url | str replace 'http+unix://' '' | url decode
let root_headers = { Authorization: $'Bearer ($root_token)', 'Content-Type': 'application/json' }
let reader_headers = { Authorization: $'Bearer ($reader.token)', 'Content-Type': 'application/json' }
let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $reader_headers $'http://localhost/sandboxes/($sandbox)/destroy' '{"location":"remote"}'
assert equal $output.status 404 "sandbox read permission must not authorize destruction"
let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $root_headers $'http://localhost/sandboxes/($sandbox)/destroy' '{"location":"local"}'
assert equal $output.status 404 "an explicit local location must not match a remote-owned sandbox"
tg --url $runner.url --token $root_token grant $reader.user.id write $sandbox

let watch = tg --url $remote.url --token $root_token checkpoint watch sandbox.control.request --params '{"kind":"destroy"}' | from json | get watch
let report = tg --url $remote.url --token $root_token checkpoint watch sandbox.control.destroy | from json | get watch
let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $reader_headers $'http://localhost/sandboxes/($sandbox)/destroy' '{"location":"remote"}'
assert equal $output.status 200
timeout 10s tg --url $remote.url --token $root_token checkpoint wait sandbox.control.destroy $report 0 | ignore

# Repeated destruction remains local while the owner has not committed the destruction report.
let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $reader_headers $'http://localhost/sandboxes/($sandbox)/destroy' '{"location":"remote"}'
assert equal $output.status 409
let hit = timeout 1s tg --url $remote.url --token $root_token checkpoint wait sandbox.control.request $watch 0 | complete
tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.request $watch
tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.destroy $report
assert equal $hit.exit_code 124 "local destruction must not contact owner-side control"
assert equal (timeout 30s tg --url $remote.url --token $root_token sandbox wait $sandbox | from json) destroyed
