use ../test.nu *

let root_token = random chars
let config = {
	advanced: {
		single_process: false,
	},
	authentication: { root: { token: $root_token } },
	roles: [cleaner http indexer scheduler],
	scheduler: {
		runner_ttl: 3,
	},
}
let remote = spawn --name remote --cloud --config $config
let created = tg --url $remote.url --token $root_token runner create | from json

let runner = spawn --name runner --config {
	runner: {
		id: $created.runner.id
		remote: "default"
		token: $created.token.token
	}
	remotes: {
		default: {
			token: $created.token.token
			url: $remote.url
		}
	}
}

let local = spawn --name local --config {
	remotes: {
		default: {
			token: $root_token
			url: $remote.url
		}
	}
}
let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(60);
		}
	'
}

let process = tg --url $local.url run --detach $path --remote
sleep 3sec

let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid

# Wait for the runner to stop.
if $nu.os-info.name == "linux" { ^tail --pid $pid -f /dev/null } else { while (ps | where pid == $pid | is-not-empty) { sleep 10ms } }

let output = tg --url $local.url process wait $process | complete
snapshot $output.stdout '
	{"error":{"code":"heartbeat_expiration","message":"heartbeat expired"},"exit":1}

'
snapshot $output.stderr ''
