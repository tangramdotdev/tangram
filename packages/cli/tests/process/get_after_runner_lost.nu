use ../../test.nu *

# Getting a process whose runner is gone must return the record the index already
# has. The get races the index against a control request to the runner, and when
# the index answers first with a process it has not yet seen finish, it blocks on
# a runner that will never answer.

let root_token = random chars
let remote = server spawn --name remote --config {
	authentication: { root: { token: $root_token } },
	roles: [cleaner http indexer scheduler],
	scheduler: {
		heartbeat_ttl: 10.0,
		runner_ttl: 10.0,
	},
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: {
		default: {
			token: $created.token.token,
			url: $remote.url,
		},
	},
	runner: {
		cpus: 1,
		id: $created.runner.id,
		memory: 1_073_741_824,
		remote: "default",
		token: $created.token.token,
	},
}
let local = server spawn --name local --config {
	remotes: {
		default: {
			token: $root_token,
			url: $remote.url,
		},
	},
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(120);
		}
	'
}

let id = tg --url $local.url build --remote --detach $path | str trim
let started = tg --url $remote.url --token $root_token get $id | from json
assert ($started.status == 'started') "the process must be started"

# Lose the runner. The index keeps the started process until the scheduler expires
# its heartbeat, so the get has an answer to give the whole time.
job kill $runner.job

let start = (date now)
let process = tg --url $remote.url --token $root_token get $id | from json
let elapsed = (((date now) - $start) / 1sec | math round -p 2)
assert ($elapsed < 5) $"getting a process without a runner took ($elapsed)s"
assert ($process.command == $started.command) "the get must return the process"
assert ($process.status == 'started') "the get must return the indexed process state"
