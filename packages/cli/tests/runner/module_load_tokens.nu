use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A loaded module must retain the tokens needed to push a child command.
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

# Reproduce a child command containing a module artifact.
let path = artifact {
	tangram.ts: 'export default async () => await tg.build(child); export function child() { return 1; }',
}
let output = timeout 20s tg --url $local.url build --remote $path | complete
success $output 'the parent should be able to wait for its child'
assert equal ($output.stdout | str trim) '1'

# A cycle makes the modules use graph pointers and exercises static and repeated dynamic imports.
let path = artifact {
	tangram.ts: '
		import { child } from "./child.tg.ts";
		export const value = 1;
		export default async () => {
			await import("./child.tg.ts");
			return await tg.build(child);
		};
	',
	child.tg.ts: '
		import { value } from "./tangram.ts";
		export function child() { return value; }
	',
}
let output = timeout 20s tg --url $local.url build --remote $path | complete
success $output 'the graph module should retain its tokens after another import'
assert equal ($output.stdout | str trim) '1'
