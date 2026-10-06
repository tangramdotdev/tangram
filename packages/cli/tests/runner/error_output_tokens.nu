use ../lib/test.nu *

# The runner must retain the error's tokens when pushing it to the remote.
let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	verification: { permissions: { initial: false, final: false } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
	verification: { permissions: { initial: false, final: false } },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let watch = tg --url $runner.url checkpoint watch runner.process.output.push.finished | from json | get watch
let path = artifact { tangram.ts: 'export default async () => { throw await tg.error({ message: "expected build failure", stack: null }); };' }
let process = tg --url $local.url build --remote --detach $path
success (timeout 15s tg --url $runner.url checkpoint wait runner.process.output.push.finished $watch 0 | complete) 'the runner should successfully push the error'
tg --url $runner.url checkpoint unwatch runner.process.output.push.finished $watch
let output = timeout 15s tg --url $local.url process output $process | complete
assert ($output.stderr | str contains 'expected build failure') 'the original build error should be readable'
