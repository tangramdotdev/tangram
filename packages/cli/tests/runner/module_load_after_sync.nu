use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A delayed import must retain authorization after the command sync expires.
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
				await tg.sleep(15);
				const { child } = await import("./child.tg.ts");
				return await tg.build(child);
			};
		',
		child.tg.ts: $child,
	}
	let output = timeout 90s tg --url $local.url build --remote $path | complete
	success $output 'a later import should retain command authorization'
	assert equal ($output.stdout | str trim) '1'
}
