use ../../test.nu *

# A remote runner must be able to push a parent process output that reuses an entry of a child's output directory. The entry referent carries no token of its own, so the parent's output is stored with only a node grant and the push reports the entry as missing.

let root_token = random chars

# Spawn the remote. It authenticates users and schedules work but holds no runner role, so the build can only complete by way of the separate runner.
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [cleaner http indexer scheduler],
}

# Create the runner and its token.
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner.
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.runner.id, remote: 'default', token: $created.token.token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Run a build whose output reuses an entry of a child's output directory.
let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.build(createDirectory).then(tg.Directory.expect);
			let entries = await directory.entries;
			return tg.directory({ reused: entries["child"] });
		}

		export function createDirectory() {
			let directory = tg.directory({ leaf: tg.file("hi") });
			for (let i = 0; i < 18; i++) {
				directory = tg.directory({ child: directory });
			}
			return directory;
		}
	'
}
let result = tg --url $local.url build --remote $path | complete
success $result "the runner must push an output that reuses an entry of a child's output directory."

# Verify the user can read the pushed output and its children.
let directory = $result.stdout | str trim
let output = tg --url $local.url get $directory --depth inf | complete
success $output "the user must read the pushed output."
