use ../lib/test.nu *

# Returning a directory must preserve the syncs of files produced on other runners.
let root_token = random chars
let store = { object_max_batch: 1 }
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	sync: { get: { store: { lmdb: $store, memory: $store, scylla: $store } } },
}
let runners = 1..3 | each {
	let created = tg --url $remote.url --token $root_token runner create | from json
	server spawn --config {
		advanced: { checkpoints: true },
		remotes: { default: { token: $created.token.token, url: $remote.url } },
		roles: [api indexer runner],

		runner: { cpus: 1, id: $created.data.id, remote: 'default', token: $created.token.token },
	}
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
# Compute the blob IDs without making their contents available to checkout.
let source = server spawn --name source
let path = artifact {
	tangram.ts: '
		export default async function () {
			const first = await tg.build(produce, "first");
			const second = await tg.build(produce, "second");
			return tg.directory({ first, second });
		}
		export function produce(text: string) {
			return tg.file(text);
		}
	'
}
let watches = [first second] | each {|text|
	let value = ['tg.blob(' ($text | to json) ')'] | str join
	let blob = tg --url $source.url put $value | str trim
	let params = { id: $blob } | to json --raw
	let store = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params $params | from json | get watch
	{ store: $store }
}
let capacities = $runners | each {|runner|
	let watch = tg --url $runner.url checkpoint watch runner.sandbox.capacity.release | from json | get watch
	{ url: $runner.url, watch: $watch }
}
let build = job spawn {
	let id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $id 0
}
for watch in $watches {
	success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $watch.store 0 | complete) 'both producers must start pushing'
}
let output = job recv --tag $build --timeout 30sec
success $output
let referent = $output.stdout | str trim
let ack = tg --url $remote.url --token $root_token checkpoint watch sync.control.ack | from json | get watch
let checkout_path = $env.TMPDIR | path join checkout
let checkout = job spawn {
	let id = job id
	let output = tg --url $local.url checkout $referent --path $checkout_path | complete
	$output | job send --tag $id 0
}
assert equal (try { job recv --tag $checkout --timeout 1sec } catch { null }) null 'checkout must wait for the output transfers'
# Checkout can wait on a file before discovering its blob.
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.control.ack $ack 0 | complete) 'checkout must wait on the output sync'
tg --url $remote.url --token $root_token checkpoint unwatch sync.control.ack $ack
for watch in $watches {
	tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $watch.store
}
success (job recv --tag $checkout --timeout 60sec)
assert equal (open --raw ($checkout_path | path join first)) first
assert equal (open --raw ($checkout_path | path join second)) second
for entry in $capacities {
	tg --url $entry.url checkpoint unwatch runner.sandbox.capacity.release $entry.watch
}
