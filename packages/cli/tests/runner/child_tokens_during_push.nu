use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A child process that starts before its parent's command has finished being pushed to the remote must still be authorized by its tokens, when neither the remote nor the runner may search for authorization.

let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	# Store objects independently so holding padding does not prevent the child from reading data.
	sync: { get: { store: {
		lmdb: { object_concurrency: 4, object_max_batch: 1 },
		scylla: { object_concurrency: 4, object_max_batch: 1 },
	} } },
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

# The modules form one graph. The runner fetches objects lazily, so the parent never fetches the contents of data.txt, and only the child reads it.
let path = artifact {
	tangram.ts: 'import { child } from "./child.tg.ts";
import "./pad.txt" with { type: "file" };
export default async () => await tg.build(child);
',
	child.tg.ts: 'import "./tangram.ts";
import data from "./data.txt" with { type: "file" };
export const child = async () => await data.text();
',
	data.txt: 'hello',
	pad.txt: 'padding',
}

# Hold the store of a blob that only the parent imports, so the parent's command push is still in progress when the child starts.
let pad = tg --url $local.url put --no-tokens 'tg.blob("padding")' | referent node
let store = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $pad } | to json --raw) | from json | get watch
let build = job spawn {
	let job_id = job id
	let output = timeout 60s tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $store 0 | complete) 'the push should reach the held blob'
let output = job recv --tag $build --timeout 90sec
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $store
success $output 'the child should be authorized by its tokens while the parent command push is in progress'
assert equal ($output.stdout | str trim) '"hello"'
