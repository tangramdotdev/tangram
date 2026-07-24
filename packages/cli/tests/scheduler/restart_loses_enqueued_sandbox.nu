use ../../test.nu *

# A sandbox create acknowledged by a scheduler is lost when that scheduler dies, so its parent fails instead of replaying the create on the replacement scheduler.

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
	process: {
		spawn_connection_timeout: 5,
	},
	roles: [cleaner finalizer http indexer scheduler],
}
let remote = spawn --name remote --config $config

let runner = spawn --name runner --config {
	remotes: {
		default: {
			url: $remote.url,
		},
	},
	runner: {
		cpus: 1,
		remote: "default",
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
			await using blocker = await tg.spawn(block).sandbox();
			const promise = Promise.resolve(tg.build(child).sandbox());
			await tg.sleep(0.25);
			console.log("child spawn is pending");
			return await promise;
		}

		export async function block() {
			await tg.sleep(2);
		}

		export function child() {
			return 42;
		}
	',
}

let process = tg --url $local.url build --detach --remote $path | str trim
wait_until {
	let output = tg --url $local.url process log --stream stdout $process | complete
	$output.exit_code == 0 and ($output.stdout | str contains "child spawn is pending")
} "the child spawn was not enqueued"

let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 9 $pid
if $nu.os-info.name == "linux" {
	^tail --pid $pid -f /dev/null
} else {
	while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
}

spawn --directory $remote.directory --name remote --config $config --url $remote.url
let replacement_runner = spawn --name replacement_runner --config {
	remotes: {
		default: {
			url: $remote.url,
		},
	},
	runner: {
		cpus: 1,
		remote: "default",
	},
}

let output = tg --url $local.url wait $process | from json
assert equal $output.exit 1
