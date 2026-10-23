use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A delayed import must retain authorization after the command sync expires.
let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	sync: { service: { attempt_ttl: 0.1 } },
	verification: { permissions: { initial: false, final: false } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
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

# Exercise both ordinary files and modules represented by graph pointers.
for graph in [false true] {
	let child = if $graph {
		'import { value } from "./tangram.ts"; export function child() { return value; }'
	} else {
		'export function child() { return 1; }'
	}
	let path = artifact {
		tangram.ts: '
			export const value = 1;
			export default async () => {
				await tg.build(gate);
				const { child } = await import("./child.tg.ts");
				return await tg.build(child);
			};
			export function gate() {}
		',
		child.tg.ts: $child,
	}

	# Start the build and find the sync of its command.
	let command_watch = tg --url $runner.url checkpoint watch runner.process.command.sync.started | from json | get watch
	let start_watch = tg --url $runner.url checkpoint watch runner.process.start | from json | get watch
	let build = job spawn {
		let id = job id
		timeout 90s tg --url $local.url build --remote $path | complete | job send --tag $id 0
	}
	tg --url $runner.url checkpoint wait runner.process.start $start_watch 0 | ignore
	tg --url $runner.url checkpoint continue runner.process.start $start_watch 0
	let sync = tg --url $runner.url checkpoint wait runner.process.command.sync.started $command_watch 0 | from json | get params.sync
	let params = { subject: $"syncs.($sync).service" } | to json --raw
	let stopped_watch = tg --url $remote.url --token $root_token checkpoint watch sync.service.stopped --params $params | from json | get watch
	tg --url $runner.url checkpoint continue runner.process.command.sync.started $command_watch 0
	tg --url $runner.url checkpoint unwatch runner.process.command.sync.started $command_watch

	# Hold the gate child, which delays the import, until the command sync expires.
	tg --url $runner.url checkpoint wait runner.process.start $start_watch 1 | ignore
	tg --url $remote.url --token $root_token checkpoint wait sync.service.stopped $stopped_watch 0 | ignore
	tg --url $remote.url --token $root_token checkpoint continue sync.service.stopped $stopped_watch 0
	tg --url $remote.url --token $root_token checkpoint unwatch sync.service.stopped $stopped_watch
	tg --url $runner.url checkpoint continue runner.process.start $start_watch 1
	tg --url $runner.url checkpoint unwatch runner.process.start $start_watch

	# The import succeeds after the command sync expires.
	let output = job recv --tag $build --timeout 90sec
	success $output 'a later import should retain command authorization'
	assert equal ($output.stdout | str trim) '1'
}
