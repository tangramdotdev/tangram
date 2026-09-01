use ../../test.nu *

# A remote runner must be able to push the error of a process whose child failed. The parent's error names the child's error object by id, the parent holds no grant on it, so the parent's error object is stored with only a node grant and the push reports the child's error object as missing.

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

# Run a build whose child throws, so the parent's error names the child's error object by id.
let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build(child);
		}
		export function child() {
			throw new Error("the child failed");
		}
	'
}
let result = tg --url $local.url build --remote $path | complete
failure $result "the build must fail because the child threw."

# The failure must report the child error, not an authorization denial disguised as a missing object.
assert (not ($result.stderr | str contains 'failed to push the process output')) $"the parent must push its error: ($result.stderr)"
assert ($result.stderr | str contains 'the child failed') $"the child error must reach the user: ($result.stderr)"
