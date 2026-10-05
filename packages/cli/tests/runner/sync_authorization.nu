use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A sync must authorize a remote build even while index verification is blocked.
let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
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
	vfs: true,
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let path = artifact { tangram.ts: 'export default () => 1;' }
let watch = tg --url $remote.url --token $root_token checkpoint watch verification.index.wait | from json | get watch
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}
success (timeout 10s tg --url $remote.url --token $root_token checkpoint wait verification.index.wait $watch 0 | complete) 'verification should reach the blocked index attempt'
let output = job recv --tag $build --timeout 10sec
success $output 'the sync should authorize the build without waiting for indexing'
assert equal ($output.stdout | str trim) '1'
tg --url $remote.url --token $root_token checkpoint unwatch verification.index.wait $watch
