use ../lib/test.nu *

# The process data stored in the remote index carries each sync token once, on the outer command and output referents, rather than once per output object.

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

# Get the stored process. Reading it creates a read token for each output object, so only the sync tokens come from the stored record.
let data = tg --url $remote.url --token $root_token process get --source=index --local $process | from json
assert equal $data.status finished
assert equal ($data.output.node | length) $count
def is-sync [] { $in | token body | get resource | str starts-with 'syn_' }

# The output objects carry no sync tokens of their own.
let inner = $data.output.node | each {|item| $item.value | referent tokens local | where { is-sync } } | flatten
assert equal ($inner | length) 0 'the output object referents should not repeat the sync token'

# The outer output referent carries the outcome sync token once.
let outer = $data.output.options.tokens | values | flatten | where { is-sync }
assert equal ($outer | length) 1 'the output referent should carry the outcome sync token once'

# The command referent carries the sync tokens of the command syncs, which are distinct from the outcome sync.
let command = $data.command.options.tokens | values | flatten | where { is-sync } | each { token body | get resource }
let outcome = $outer.0 | token body | get resource
assert (($command | length) >= 1) 'the command referent should carry the command sync token'
assert (($command | length) <= 2) 'the command referent should carry only the spawn and control command sync tokens'
assert ($outcome not-in $command) 'the command referent should not carry the outcome sync token'
