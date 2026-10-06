use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A sync must authorize a module backed by a graph while index verification is blocked.
let root_token = random chars
let store = { object_max_batch: 1 }
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	sync: { get: { store: { lmdb: $store, memory: $store, scylla: $store } } },
	verification: { permissions: { initial: false, final: false } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
	verification: { permissions: { initial: false, final: false } },
	vfs: true,
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let path = artifact {
	tangram.ts: 'import "./other.tg.ts"; export default () => 1;',
	other.tg.ts: 'import "./tangram.ts"; export const other = 2;',
}
# Build the file by ID so loading its contents must follow the pointer to the graph.
let file = tg --url $local.url checkin ($path | path join tangram.ts) | referent node
let graph = tg --url $local.url get --no-tokens $file | parse --regex '(?<id>gph_[a-z0-9]+)' | first | get id
let graph_watch = tg --url $remote.url --token $root_token checkpoint watch verification.index --params ({ resource: $graph } | to json --raw) | from json | get watch
# Hold a descendant until the graph is requested so the file is authorized before its subtree.
let blob = tg --url $local.url put --no-tokens 'tg.blob("import \"./tangram.ts\"; export const other = 2;")' | referent node
let store_watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blob } | to json --raw) | from json | get watch
let watch = tg --url $remote.url --token $root_token checkpoint watch verification.index.wait | from json | get watch
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $file | complete
	$output | job send --tag $job_id 0
}
success (timeout 10s tg --url $remote.url --token $root_token checkpoint wait verification.index.wait $watch 0 | complete) 'verification should reach the blocked index attempt'
success (timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $store_watch 0 | complete) 'the upload should reach the held blob'
let graph_request = timeout 10s tg --url $remote.url --token $root_token checkpoint wait verification.index $graph_watch 0 | complete
success $graph_request 'the runner should request the backing graph'
let token_resource = $graph_request.stdout | from json | get params.token_resource
assert ($token_resource | str contains 'syn_') 'the graph request should retain the authorization token for the sync'
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $store_watch
tg --url $remote.url --token $root_token checkpoint unwatch verification.index $graph_watch
let output = job recv --tag $build --timeout 10sec
success $output 'the sync should authorize the build without waiting for indexing'
assert equal ($output.stdout | str trim) '1'
tg --url $remote.url --token $root_token checkpoint unwatch verification.index.wait $watch
