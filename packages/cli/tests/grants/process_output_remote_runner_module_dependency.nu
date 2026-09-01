use ../../test.nu *

# A remote runner must push a cache-hit child's output when the module graph has a cycle.

let root_token = random chars

# Spawn the remote. It holds no runner role, so the build can only complete by way of the runner.
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

# tangram.ts and dep.tg.ts import each other. foo and bar both build dep, so one of them receives
# it as a cache hit. dep returns the imported file rather than an object it constructs.
let path = artifact {
	tangram.ts: '
		import dep from "./dep.tg.ts";
		import file from "./file.txt";

		export default () => {
			return tg.directory({
				foo: tg.build(foo),
				bar: tg.build(bar),
			})
		}

		export const foo = () => {
			return tg.build(dep);
		};

		export const bar = () => {
			return tg.build(dep);
		};
	',
	dep.tg.ts: '
		import * as root from "./tangram.ts";
		import file from "./file.txt";
		export default async () => {
			await tg.sleep(5);
			return file;
		};
	',
	file.txt: 'hello, world asdfad',
}

let result = tg --url $local.url build --remote $path | complete
success $result "the runner must push a cache-hit child output through a cyclic module graph."

let directory = $result.stdout | str trim
let output = tg --url $local.url get $directory --depth inf | complete
success $output "the user must read the pushed output."
