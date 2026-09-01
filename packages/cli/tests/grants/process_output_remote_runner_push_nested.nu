use ../../test.nu *

# A remote runner must be able to push a parent process output assembled from its children's outputs, and the user who spawned the build must be able to read every object in it.

let root_token = random chars

# Spawn the remote. It authenticates users and schedules work but holds no runner role, so the build can only complete by way of the separate runner.
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}

# Create the runner and its token.
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner.
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.data.id, remote: 'default', token: $created.token.token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Run a build whose output is a directory assembled from two children's outputs.
let path = artifact {
	tangram.ts: '
		export default async function () {
			let a = await tg.build(one);
			let b = await tg.build(two);
			return tg.directory({ one: a, two: b });
		}
		export function one() {
			return tg.file("one");
		}
		export function two() {
			return tg.directory({ leaf: tg.file("two") });
		}
	'
}
let result = tg --url $local.url build --remote $path | complete
success $result "the runner must push an output assembled from its children's outputs."

# Verify the user can read the pushed output and its children.
let directory = $result.stdout | str trim
let output = tg --url $local.url get $directory --depth inf | complete
success $output "the user must read the pushed output."
