use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A parent must be able to wait for a child on a separate runner with zero search budgets.
let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { single_process: false },
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
# Use a builtin executable so the child command has no artifact dependencies.
let path = artifact {
	tangram.ts: 'export default async () => { await tg.build({ executable: "tg", args: ["--version"], host: tg.host.current }); return 1; };',
}
let output = timeout 20s tg --url $local.url build --remote $path | complete
success $output 'the parent should be able to wait for its child'
assert equal ($output.stdout | str trim) '1'
