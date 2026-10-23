use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A sandbox must wait for an imported file's contents, read them, and reject an unrelated store path while its package is still being pushed.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	# Store objects independently so holding both blobs does not hold the file objects.
	sync: { get: { store: { lmdb: { object_concurrency: 4, object_max_batch: 1 } } } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
	vfs: true,
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let other = tg --url $local.url checkin (artifact 'other') | referent node
let path = artifact {
	tangram.ts: ('
		import data from "./data.txt" with { type: "file" };
		import "./other.txt" with { type: "file" };
		export default () => tg.build`value=; read value < ${data}; test "$value" = hello && ! test -e /opt/tangram/store/OTHER`;
	' | str replace 'OTHER' $other),
	data.txt: 'hello',
	other.txt: 'other',
}

# Hold both blobs until the sandbox waits for its input.
let contents = tg --url $local.url put --no-tokens 'tg.blob("hello")' | referent node
let padding = tg --url $local.url put --no-tokens 'tg.blob("other")' | referent node
let contents_watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $contents } | to json --raw) | from json | get watch
let padding_watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $padding } | to json --raw) | from json | get watch
let wait = tg --url $runner.url checkpoint watch vfs.authorization.wait | from json | get watch
job spawn { tg --url $local.url build --remote $path | complete | job send 0 }
success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s sync.get.store.object $contents_watch 0 | complete) 'the push should reach the held contents'
success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s sync.get.store.object $padding_watch 0 | complete) 'the push should reach the held padding'
success (tg --url $runner.url checkpoint wait --timeout 30s vfs.authorization.wait $wait 0 | complete) 'the sandbox should wait for its input'

# Deliver the input while keeping the package push open through the other blob.
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $contents_watch
tg --url $runner.url checkpoint unwatch vfs.authorization.wait $wait
let output = job recv --timeout 30sec
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $padding_watch
success $output 'the sandbox should read the delivered input and reject the unrelated path while the package push is held'
