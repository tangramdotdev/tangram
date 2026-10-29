use ../../util/test.nu *

# A process's finished index write can be indexed before its started write. The started write is then rejected, and the rest of its batch, which charges the process to its owner's usage, is lost.

let root = random chars
let remote = server spawn --preserve-keys --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	usage: true,
}
let created = tg --url $remote.url --token $root runner create | from json
let runner = server spawn --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json

# Hold the process's started and finished writes, and its sandbox's destroyed write so that no other write is in flight.
let started = checkpoint watch index.batch --params { started_process: true } --url $remote.url --token $root
let finished = checkpoint watch index.batch --params { finished_process: true } --url $remote.url --token $root
let destroyed = checkpoint watch index.batch --params { destroyed_sandbox: true } --url $remote.url --token $root
let path = artifact { tangram.ts: 'export default () => tg.file("hello")' }
tg --url $remote.url --token $alice.token build --detach $path | ignore
for checkpoint in [$started $finished $destroyed] { checkpoint wait $checkpoint | ignore }

# Index the finished write, then the started write.
let committed = checkpoint watch index.batch.finished --params { finished_process: true } --url $remote.url --token $root
checkpoint unwatch $finished
checkpoint wait $committed | ignore
checkpoint unwatch $committed
checkpoint unwatch $started
checkpoint unwatch $destroyed
tg --url $remote.url --token $root index

# The process must be charged to alice.
assert equal (tg --url $remote.url --token $alice.token usage | from json | get process_count) 1
