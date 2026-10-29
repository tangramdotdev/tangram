use ../../util/test.nu *

# A runner preserves its sandboxes across a scheduler restart and assigns child work to the new scheduler.

let root_token = random chars
let config = {
	advanced: {
		multi_server: true,
	},
	authentication: { root: { token: $root_token } },
	indexer: { id: 'idx_0000000000000000000000000000' },
	roles: [api indexer scheduler],
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}
let remote = server spawn --name remote --preserve-keys --config $config
let created = tg --url $remote.url --token $root_token runner create | from json

let runner = server spawn --name runner --config {
	advanced: {
		checkpoints: true,
	},
	remotes: {
		default: {
			token: $created.token.token
			url: $remote.url,
		},
	},
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
	runner: {
		cpus: 1,
		overcommit: { cpu: 1 },
		id: $created.data.id
		remote: "default",
		scheduler_ttl: 3,
		token: $created.token.token
	},
}

let local = server spawn --name local --config {
	remotes: {
		default: {
			token: $root_token
			url: $remote.url,
		},
	},
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build(child).sandbox();
		}

		export function child() {
			return 42;
		}
	',
}

let start_watch = (
	tg --url $runner.url checkpoint watch runner.process.start
	| from json
	| get watch
)
let process = tg --url $local.url build --no-tokens --detach --remote $path | referent node

# Hold the runner after it installs the parent sandbox, then replace the
# scheduler before the parent requests its child sandbox.
tg --url $runner.url checkpoint wait runner.process.start $start_watch 0 | ignore

let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid

let remote = server start $remote

tg --url $runner.url checkpoint continue runner.process.start $start_watch 0
tg --url $runner.url checkpoint unwatch runner.process.start $start_watch

let output = tg --url $local.url wait $process | from json
assert equal $output.output 42
