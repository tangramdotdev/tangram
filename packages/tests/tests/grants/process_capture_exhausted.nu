use ../../util/test.nu *

# A runner's capture of a process's permissions whose search is exhausted completes once with the permissions that it found, so that indexing with --capture returns.

let root_token = random chars
let remote = server spawn --cloud --name remote --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	verification: { permissions: { initial: false, final: false } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
	verification: { permissions: { initial: false, final: false } },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Build a process on the runner whose output objects the runner's capture cannot prove without a search.
let path = artifact {
	tangram.ts: '
		export default async () => [await tg.file("first"), await tg.file("second")];
	',
}
let referent = tg --url $local.url build --remote --detach $path | str trim
success (tg --url $local.url wait $referent | complete) 'the build should finish'

# The runner's capture completes once with what it found, so indexing with --capture returns instead of waiting for captures that are retried.
success (timeout 30s tg --url $runner.url index --capture | complete) 'the capture should complete with what it found'
