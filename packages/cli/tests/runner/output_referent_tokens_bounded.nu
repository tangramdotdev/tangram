use ../lib/test.nu *

# The process data stored in the remote index carries no tokens on its output referents, so the stored record stays bounded by the number of objects the output references rather than growing with a sync token per referent. The remote rejects a put whose stored data carries tokens when checkpoints are enabled.

let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { checkpoints: true },
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

# The output is an array of distinct files, so the process data holds one object referent per file.
let count = 512
let module = 'export default async () => {
	const files = [];
	for (let i = 0; i < COUNT; i++) {
		files.push(await tg.file("file " + i));
	}
	return files;
};
' | str replace 'COUNT' ($count | into string)
let path = artifact { tangram.ts: $module }
let output = timeout 120s tg --url $local.url build --remote $path | complete
success $output 'the build should finish'

# The remote must not have rejected the finished process for carrying tokens in its stored data.
let errors = server_errors $remote | where { $in | str contains 'must not carry tokens' }
assert equal $errors [] 'the stored process data should carry no tokens'
