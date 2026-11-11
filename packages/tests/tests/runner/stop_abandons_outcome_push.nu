use ../../util/test.nu *

# A graceful stop of a runner abandons the push of a finished process's outcome once the push drain timeout passes, so that a stuck push does not hang the stop.

let root_token = random chars
let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	verification: { permissions: { initial: false, final: false } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, push_drain_timeout: 1, remote: "default", token: $created.token.token },
	verification: { permissions: { initial: false, final: false } },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Compute the ID of the output file, and hold its store on the remote, so that the runner's push of the outcome is in flight.
let output = tg --url $local.url put --no-tokens 'tg.file("pushed while the runner stops")' | referent node
let store = checkpoint watch sync.get.store.object --params { id: $output } --token $root_token --url $remote.url

# Build a process on the runner whose child, which the runner runs on the shortcut path, returns the file, and wait for it to finish.
let path = artifact {
	tangram.ts: '
		export default async () => [await tg.build(child)];
		export const child = () => tg.file("pushed while the runner stops");
	',
}
let referent = tg --url $local.url build --remote --detach $path | str trim
success (timeout 30s tg --url $local.url wait $referent | complete) 'the build should finish'
checkpoint wait $store | ignore

# Stop the runner gracefully while the push is held. The stop finishes once the push drain timeout passes.
let stop = job spawn {
	let id = job id
	server stop $runner
	0 | job send --tag $id 0
}
let stopped = try {
	job recv --tag $stop --timeout 20sec | ignore
	true
} catch {
	false
}
assert $stopped 'the stop should finish once the push drain timeout passes'
checkpoint unwatch $store
