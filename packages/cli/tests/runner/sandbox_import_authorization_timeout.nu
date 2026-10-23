use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# The VFS authorization timeout must also bound a remote request that does not finish.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	sync: { get: { store: { lmdb: { object_concurrency: 4, object_max_batch: 1 } } } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
	vfs: { remote_subtree_timeout: 5 },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let path = artifact {
	tangram.ts: '
		import file from "./file.txt" with { type: "file" };
		export default () => tg.build`! test -e ${file}`;
	',
	file.txt: 'hello',
}

# Hold the contents until the sandbox waits for the file.
let file = tg --url $local.url checkin (artifact 'hello') | referent node
let contents = tg --url $local.url put --no-tokens 'tg.blob("hello")' | referent node
let store = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $contents } | to json --raw) | from json | get watch
let wait = tg --url $runner.url checkpoint watch vfs.authorization.wait | from json | get watch
job spawn { tg --url $local.url build --remote $path | complete | job send 0 }
success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s sync.get.store.object $store 0 | complete) 'the push should reach the held contents'
success (tg --url $runner.url checkpoint wait --timeout 30s vfs.authorization.wait $wait 0 | complete) 'the sandbox should wait for the file'

# Hold the next remote authorization request until the sandbox lookup times out.
let request = tg --url $remote.url --token $root_token checkpoint watch verification.index --params ({ resource: $file } | to json --raw) | from json | get watch
tg --url $runner.url checkpoint unwatch vfs.authorization.wait $wait
success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s verification.index $request 0 | complete) 'the remote should receive the next authorization request'
let output = job recv --timeout 30sec
tg --url $remote.url --token $root_token checkpoint unwatch verification.index $request
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $store
success $output 'the sandbox lookup should time out while the remote request is held'
