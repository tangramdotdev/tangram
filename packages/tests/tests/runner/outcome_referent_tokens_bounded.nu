use ../../util/test.nu *

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

# The root process is scheduled and builds a child on the shortcut path, and each returns an array of distinct files, so each process's data holds one object referent per file.
let count = 512
let module = 'export default async () => {
	const files = await tg.build(child);
	files.push(await tg.file("root"));
	return files;
};

export const child = async () => {
	const files = [];
	for (let i = 0; i < COUNT; i++) {
		files.push(await tg.file("file " + i));
	}
	return files;
};
' | str replace 'COUNT' ($count | into string)
let path = artifact { tangram.ts: $module }
let command_watch = tg --url $runner.url checkpoint watch runner.process.command.sync.started | from json | get watch
let watch = tg --url $runner.url checkpoint watch runner.process.outcome.sync.finished | from json | get watch
let referent = tg --url $local.url build --remote --detach $path | str trim
let process = $referent | referent node
success (tg --url $local.url wait $referent | complete) 'the build should finish'
let command_sync = tg --url $runner.url checkpoint wait --timeout 60s runner.process.command.sync.started $command_watch 0 | from json | get params.sync
success (tg --url $runner.url checkpoint wait --timeout 60s runner.process.outcome.sync.finished $watch 0 | complete) 'the runner should push the outcome'
tg --url $runner.url checkpoint unwatch runner.process.command.sync.started $command_watch
tg --url $runner.url checkpoint unwatch runner.process.outcome.sync.finished $watch
tg --url $remote.url --token $root_token index
def is-sync [] { $in | token body | get resource | str starts-with 'syn_' }
def sync-resources [tokens: record] { $tokens | values | flatten | where { is-sync } | each { token body | get resource } }

# Get the stored processes. Reading a process creates a read token for each output object, so only the sync tokens come from the stored record.
let root = tg --url $remote.url --token $root_token process get --source=index --local $process | from json
let child = $root.children.0.process | referent node
let child = tg --url $remote.url --token $root_token process get --source=index --local $child | from json
for data in [$root $child] {
	assert equal $data.status finished

	# The output objects carry no sync tokens of their own.
	let inner = $data.output.node | each {|item| $item.value | referent tokens local | where { is-sync } } | flatten
	assert equal ($inner | length) 0 'the output object referents should not repeat the sync token'

	# The outer output referent carries the outcome sync token once.
	let outcome = sync-resources ($data.output.options.tokens? | default {})
	assert equal ($outcome | length) 1 'the output referent should carry the outcome sync token once'
}
assert equal ($child.output.node | length) $count

# The scheduled root's command referent carries only the sync of the spawn's command push, because the runner does not push a scheduled process's command.
let root_command = sync-resources ($root.command.options.tokens? | default {})
assert equal ($root_command | length) 1 'the scheduled command referent should carry only the spawn push sync token'
assert ($command_sync not-in $root_command) 'the scheduled command referent should not carry an unused command sync token'

# The shortcut child's command referent carries the sync under which the runner pushed its command objects, and the spawn push sync that delivered the module objects it shares with the root.
let child_command = sync-resources ($child.command.options.tokens? | default {})
assert equal ($child_command | sort) ([$command_sync $root_command.0] | sort) 'the shortcut command referent should carry the command sync and spawn push sync tokens'
