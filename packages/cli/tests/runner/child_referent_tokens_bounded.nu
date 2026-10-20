use ../lib/test.nu *

# The child referents stored for a finished process carry only tokens for the child itself, so the parent's tokens are not copied onto every child. The remote rejects a put whose stored child referents carry other tokens when checkpoints are enabled.

let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	tracing: { stderr_format: 'json' },
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

# The parent spawns many children and returns their outputs, so its stored data holds one child referent per child.
let count = 32
let module = 'export const child = (i: number) => tg.file("file " + i);
export default async () => {
	const files = [];
	for (let i = 0; i < COUNT; i++) {
		files.push(await tg.build(child, i));
	}
	return files;
};
' | str replace 'COUNT' ($count | into string)
let path = artifact { tangram.ts: $module }
let output = timeout 180s tg --url $local.url build --remote $path | complete
success $output 'the build should finish'

# The remote must not have rejected any finished process for carrying tokens in its stored data or child referents.
let errors = server_errors $remote | where { $in | str contains 'must not carry' }
assert equal $errors [] 'the stored process data and child referents should carry only their own tokens'
