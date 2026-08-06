use ../../test.nu *

# A sandbox create acknowledged by a scheduler is lost when that scheduler dies, so its parent fails instead of replaying the create on the replacement scheduler.

let root_token = random chars
let config = {
	advanced: {
		single_process: false,
	},
	authentication: { root: { token: $root_token } },
	roles: [cleaner finalizer http indexer scheduler],
	scheduler: {
		heartbeat_ttl: 3,
	},
}
let remote = spawn --name remote --preserve-keys --config $config
let created = tg --url $remote.url --token $root_token runner create | from json
let replacement_created = tg --url $remote.url --token $root_token runner create | from json

let runner = spawn --name runner --config {
	remotes: {
		default: {
			token: $created.token.token
			url: $remote.url,
		},
	},
	runner: {
		cpus: 1,
		id: $created.runner.id
		remote: "default",
		token: $created.token.token
	},
	scheduler: {
		heartbeat_ttl: 3,
	},
}

let local = spawn --name local --config {
	remotes: {
		default: {
			token: $root_token
			url: $remote.url,
		},
	},
	scheduler: {
		heartbeat_ttl: 3,
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

spawn --directory $remote.directory --name remote --preserve-keys --config $config --url $remote.url
let replacement_runner = spawn --name replacement_runner --config {
	remotes: {
		default: {
			token: $replacement_created.token.token
			url: $remote.url,
		},
	},
	runner: {
		cpus: 1,
		id: $replacement_created.runner.id
		remote: "default",
		token: $replacement_created.token.token
	},
}

let output = tg --url $local.url wait $process | from json
assert equal $output.exit 1
