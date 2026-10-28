use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A sandbox on a runner with the VFS checks in an imported file's store path once the push delivers the file's contents, while the rest of its package is still being pushed.
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
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
	vfs: true,
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let path = artifact {
	tangram.ts: '
		import data from "./data.txt" with { type: "file" };
		import "./other.txt" with { type: "file" };
		export default () => tg.build`tg checkin --no-tokens ${data} > ${tg.output}`;
	',
	data.txt: 'hello',
	other.txt: 'other',
}
let file = tg --url $local.url checkin (artifact 'hello') | referent node

# Hold both blobs on the remote, and start the build.
let contents = tg --url $local.url put --no-tokens 'tg.blob("hello")' | referent node
let padding = tg --url $local.url put --no-tokens 'tg.blob("other")' | referent node
let contents_watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $contents } | to json --raw) | from json | get watch
let padding_watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $padding } | to json --raw) | from json | get watch
let retain_watch = tg --url $remote.url --token $root_token checkpoint watch sync.service.request.retain --params ({ node: $contents } | to json --raw) | from json | get watch
job spawn { tg --url $local.url build --remote $path | complete | job send 0 }
success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s sync.get.store.object $contents_watch 0 | complete) 'the push should reach the held contents'
success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s sync.get.store.object $padding_watch 0 | complete) 'the push should reach the held padding'

# Wait until the sandbox is waiting for the contents, and deliver them while keeping the package push open through the other blob.
success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s sync.service.request.retain $retain_watch 0 | complete) 'the sandbox should wait for the contents'
tg --url $remote.url --token $root_token checkpoint unwatch sync.service.request.retain $retain_watch
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $contents_watch
let output = job recv --timeout 30sec
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $padding_watch
success $output 'the checkin should finish while the package push is held'
let output = tg --url $local.url read ($output.stdout | str trim)
assert equal ($output | referent node) $file
