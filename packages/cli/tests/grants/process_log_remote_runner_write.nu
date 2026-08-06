use ../../test.nu *

# A remote runner writes the logs of a process it executes back to the remote, and the user who
# spawned that process can read them. The runner resolves the remote for the log write as itself
# rather than as the spawning user, who has no remote of that name.

let root_token = random chars

# The remote authenticates users and schedules work but holds no runner role, so the build can only
# complete by way of the separate runner.
let remote = spawn --name remote --cloud --preserve-keys --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [cleaner finalizer http indexer scheduler],
}

let created = tg --url $remote.url --token $root_token runner create | from json
let runner = spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.runner.id, remote: 'default', token: $created.token.token },
}

# Alice is an ordinary authenticated user driving her own server.
let alice = tg --url $remote.url login --verbose alice | from json
let local = spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("hello from the runner");
			return 42;
		}
	'
}

let process = tg --url $local.url build --remote --detach $path

# Draining the logs makes the runner resolve its configured remote. It has to do so as the runner:
# resolving as the spawning user finds no remote of that name, and the process then dies on the log
# write rather than on anything to do with its own work.
let wait = tg --url $local.url wait $process | complete
success $wait
snapshot $wait.stdout '
	{"exit":0,"output":42}

'

let logs = tg --url $local.url log --no-timeout $process | complete
success $logs
snapshot $logs.stdout '
	hello from the runner

'
