use ../../util/test.nu *

# A consumer waits for its inputs while both producers are still transferring them.
let root_token = random chars
let store = { object_max_batch: 1 }
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
	sync: { get: { store: { lmdb: $store, memory: $store, scylla: $store } } },
}
let runners = 1..4 | each {
	let created = tg --url $remote.url --token $root_token runner create | from json
	server spawn --config {
		advanced: { checkpoints: true },
		remotes: { default: { token: $created.token.token, url: $remote.url } },
		roles: [api indexer runner],

		scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
		runner: { cpus: 1, overcommit: { cpu: 1 }, id: $created.data.id, remote: 'default', token: $created.token.token },
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
			const first = await tg.build(produce, "first");
			const second = await tg.build(produce, "second");
			const inputs = await tg.directory({ second, first });
			const output = await tg.build(consume, inputs);
			await stealer.cancel();
			return output;
		}
		export async function steal() {
			await tg.sleep(60);
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
# Hold each file's contents at the remote.
let watches = [first second] | each {|text|
	let value = ['tg.blob(' ($text | to json) ')'] | str join
	let blob = tg --url $local.url put --no-tokens $value | referent node
	let params = { id: $blob } | to json --raw
	tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params $params | from json | get watch
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
	success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s sync.get.store.object $watch 0 | complete) 'both producers must start pushing'
}
assert equal (try { job recv --tag $build --timeout 0sec } catch { null }) null 'the build must wait for the input transfers'

# Release the transfers. The consumer reads both inputs.
for watch in $watches {
	tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $watch
}
let output = job recv --tag $build --timeout 60sec
success $output
assert equal ($output.stdout | from json) 'second first'
for entry in $capacities {
	tg --url $entry.url checkpoint unwatch runner.sandbox.capacity.release $entry.watch
}
