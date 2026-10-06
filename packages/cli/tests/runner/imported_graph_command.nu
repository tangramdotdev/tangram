use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A separate runner can push an imported graph using objects already stored at the API.
let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
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

# A module outside the cycle builds a function from the graph before every descendant is authorized locally.
let path = artifact {
	tangram.ts: '
		import { child } from "./child.tg.ts";
		export default async () => await tg.build(child);
	',
	child.tg.ts: '
		import { value } from "./cycle.tg.ts";
		export function child() { return value; }
	',
	cycle.tg.ts: '
		import { child } from "./child.tg.ts";
		import data from "./data" with { type: "directory" };
		export const value = 1;
	',
	data: (0..100 | each { |i| { name: ($i | into string), value: ($i | into string) } } | transpose -r -d),
}
let output = timeout 30s tg --url $local.url build --remote --cached=false $path | complete
success $output 'the parent should wait for the child'
assert equal ($output.stdout | str trim) '1'
