use ../../test.nu *

# A runner preserves its sandboxes across a scheduler restart and assigns child work to the new scheduler.

let private_key = mktemp
let public_key = mktemp
'U9ZBC697GDA0dlUBF/VVM4eqoJUVfQqwRNr6L2z8Ajg=' | decode base64 | save -f $private_key
'MKmfiiYtaN4W/pP+V2hmmjtT2/+ILjYfiMJ9y4EsG1U=' | decode base64 | save -f $public_key
let keys = {
	private_key: {
		algorithm: "ed25519",
		name: "default",
		path: $private_key,
	},
	public_keys: [{
		algorithm: "ed25519",
		name: "default",
		path: $public_key,
	}],
}
let config = {
	advanced: {
		single_process: false,
	},
	authentication: {
		tokens: $keys,
	},
	grants: {
		tokens: $keys,
	},
	roles: [cleaner finalizer http indexer scheduler],
}
let remote = spawn --name remote --config $config

let runner = spawn --name runner --config {
	advanced: {
		checkpoints: true,
	},
	remotes: {
		default: {
			url: $remote.url,
		},
	},
	runner: {
		cpus: 1,
		remote: "default",
		scheduler_ttl: 3,
	},
}

let local = spawn --name local --config {
	remotes: {
		default: {
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
let process = tg --url $local.url build --detach --remote $path | str trim

# Hold the runner after it installs the parent sandbox, then replace the
# scheduler before the parent requests its child sandbox.
tg --url $runner.url checkpoint wait runner.process.start $start_watch 0 | ignore

let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 9 $pid
if $nu.os-info.name == "linux" {
	^tail --pid $pid -f /dev/null
} else {
	while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
}

spawn --directory $remote.directory --name remote --config $config --url $remote.url

tg --url $runner.url checkpoint continue runner.process.start $start_watch 0
tg --url $runner.url checkpoint unwatch runner.process.start $start_watch

let output = tg --url $local.url wait $process | from json
assert equal $output.output 42
