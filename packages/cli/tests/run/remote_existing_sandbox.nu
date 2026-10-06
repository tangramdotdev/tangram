use ../lib/test.nu *

# A local server can run a process remotely in an existing sandbox created on the remote.

let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: 'export default () => "hello";
',
}

let sandbox = tg --url $local.url sandbox create --remote | str trim
let output = tg --url $local.url run --remote $'--sandbox=($sandbox)' $path | complete
success $output "the run should succeed"
assert equal ($output.stdout | str trim) '"hello"'
