use ../lib/test.nu *

# The process data stored in the remote index must not repeat the outcome sync token on every output referent, because the stored record grows with the number of objects the output references.

let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	verification: { permissions: { initial: false, final: false } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
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
let watch = tg --url $runner.url checkpoint watch runner.process.outcome.sync.finished | from json | get watch
let referent = tg --url $local.url build --remote --detach $path | str trim
let process = $referent | referent node
success (tg --url $local.url wait $referent | complete) 'the build should finish'
success (timeout 60s tg --url $runner.url checkpoint wait runner.process.outcome.sync.finished $watch 0 | complete) 'the runner should push the outcome'
tg --url $runner.url checkpoint unwatch runner.process.outcome.sync.finished $watch
tg --url $remote.url --token $root_token index

# Reading the process creates a token for each output object, so only the sync tokens on the referents come from the stored record.
let data = tg --url $remote.url --token $root_token process get --source=index --local $process | from json
assert equal $data.status finished
let sync_tokens = $data.output | each {|item|
	$item.value | referent tokens local | where {|token|
		$token | token body | get resource | str starts-with 'syn_'
	}
} | flatten
let bytes = $sync_tokens | each {|token| $token | str length } | math sum
print $'referents: ($data.output | length), sync tokens: ($sync_tokens | length), distinct: ($sync_tokens | uniq | length), bytes: ($bytes)'
assert equal ($data.output | length) $count
assert (($sync_tokens | length) <= 2) $'the stored output referents should not repeat the sync token, found ($sync_tokens | length) copies'
