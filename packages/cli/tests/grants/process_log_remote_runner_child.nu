use ../../test.nu *

# A remote runner writes the logs of a child process it executes back to the remote, and the user who spawned the parent build can read them. The user holds no grant on the child directly, so reading the child log must flow from the parent process through the sandbox owner.

let root_token = random chars

# The remote authenticates users and schedules work but holds no runner role, so the build can only complete by way of the separate runner.
let remote = server spawn --name remote --cloud --preserve-keys --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [cleaner http indexer scheduler],
}

let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.runner.id, remote: 'default', token: $created.token.token },
}

# Alice is an ordinary authenticated user driving her own server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			console.log("hello from the parent");
			return await tg.build(child);
		}
		export function child() {
			console.log("hello from the child");
			return 42;
		}
	'
}

let process = tg --url $local.url build --remote --detach $path

let wait = tg --url $local.url wait $process | complete
success $wait
snapshot $wait.stdout '
	{"exit":0,"output":42}

'

# The user must read the parent log.
let logs = tg --url $local.url log --no-timeout $process | complete
success $logs
snapshot $logs.stdout '
	hello from the parent

'

# The user must read the child log through the parent.
let children = tg --url $local.url process children $process | complete
success $children "the user must list the children of her own process."
let child = $children.stdout | from json | get 0.process
let child_logs = tg --url $local.url log --no-timeout $child | complete
success $child_logs "the user must read the log of a child process run on the remote runner."
snapshot $child_logs.stdout '
	hello from the child

'
