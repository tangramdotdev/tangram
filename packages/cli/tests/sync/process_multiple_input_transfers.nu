use ../lib/test.nu *

# A directory's sync waits on both producer syncs when its files are still transferring.
let root_token = random chars
let store = { object_max_batch: 1 }
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	sync: { get: { store: { lmdb: $store, memory: $store, scylla: $store } } },
}
let runners = 1..4 | each {
	let created = tg --url $remote.url --token $root_token runner create | from json
	server spawn --config {
		advanced: { checkpoints: true },
		process: { await_push: false },
		remotes: { default: { token: $created.token.token, url: $remote.url } },
		roles: [api indexer runner],

		runner: { cpus: 1, id: $created.data.id, remote: 'default', token: $created.token.token },
	}
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let path = artifact {
	tangram.ts: '
		export default async function () {
			const stealer = await tg.build(steal).spawn();
			await tg.sleep(1);
			const first = await tg.build(produce, "first");
			const second = await tg.build(produce, "second");
			const inputs = await tg.directory({ second, first });
			const output = await tg.build(consume, inputs);
			await stealer.wait();
			return output;
		}
		export async function steal() {
			await tg.sleep(20);
		}
		export function produce(text: string) {
			return tg.file(text);
		}
		export async function consume(inputs: tg.Directory) {
			const files = await Promise.all([inputs.get("second"), inputs.get("first")]);
			const texts = await Promise.all(files.map((file) => {
				tg.File.assert(file);
				return file.text;
			}));
			return texts.join(" ");
		}
	'
}
# Hold each file's contents at the remote and observe requests to its producer sync.
let watches = [first second] | each {|text|
	let value = ['tg.blob(' ($text | to json) ')'] | str join
	let blob = tg --url $local.url put $value | str trim
	let params = { id: $blob } | to json --raw
	let store = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params $params | from json | get watch
	let params = { node: $blob } | to json --raw
	let ack = tg --url $remote.url --token $root_token checkpoint watch sync.control.ack --params $params | from json | get watch
	{ store: $store, ack: $ack }
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
# Both producer transfers must remain unfinished when the consumer needs their contents.
for watch in $watches {
	success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $watch.store 0 | complete) 'both producers must start pushing'
}
let ack_tag = random int 1000000..2000000
for watch in $watches {
	job spawn {
		let output = timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.control.ack $watch.ack 0 | complete
		{ output: $output, watch: $watch } | job send --tag $ack_tag 0
	} | ignore
}
for _ in $watches {
	let event = job recv --tag $ack_tag --timeout 35sec
	success $event.output 'each input must wait on its own producer transfer'
	assert equal (try { job recv --tag $build --timeout 1sec } catch { null }) null 'the build must wait for the input transfers'
	let watch = $event.watch
	tg --url $remote.url --token $root_token checkpoint unwatch sync.control.ack $watch.ack
	tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $watch.store
}
let output = job recv --tag $build --timeout 60sec
success $output
assert equal ($output.stdout | from json) 'second first'
for entry in $capacities {
	tg --url $entry.url checkpoint unwatch runner.sandbox.capacity.release $entry.watch
}
