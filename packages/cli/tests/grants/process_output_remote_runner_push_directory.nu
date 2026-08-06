use ../../test.nu *

# A remote runner must be able to push a child process's output that names, by id, a file created by a sibling child process. The second child holds no grant on the first child's output, so storing its output grants only the node and the push reports the file as missing.

skip_if_offline

let root_token = random chars

# Spawn the remote.
let remote = spawn --busybox --cloud --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [cleaner finalizer http indexer scheduler],
}

# Create the runner and its token.
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner.
let runner = spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.runner.id, remote: "default", token: $created.token.token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose alice | from json
let local = spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Run a build where one child process creates a file and a second child process returns it by id.
let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async function () {
			const file = await tg.build(producer);
			return await tg.build(consumer, { id: file.id });
		}
		export function producer() {
			return tg.run`echo hello > $TANGRAM_OUTPUT`.env(tg.build(busybox)).then(tg.File.expect);
		}
		export function consumer(arg) {
			return tg.File.withId(arg.id);
		}
	'
}
let result = tg --url $local.url build --remote $path | complete
success $result "the runner must push a child process output that names a sibling child's file by id."

# Verify the user can read the output.
let directory = $result.stdout | str trim
let output = tg --url $local.url get $directory | complete
success $output "the user must read the pushed output."
