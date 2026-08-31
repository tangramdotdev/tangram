use ../test.nu *

# After a remote runner produces a process output, restarting the remote server preserves the process log so it can still be retrieved from the local server.

# Spawn a server in a given directory.
let root_token = random chars
let config =  { 
	advanced: {
		single_process: false,
	}
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let remote = server spawn --name remote --cloud --config $config
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn a remote runner.
let runner = server spawn --name runner --config {
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

# Spawn a local server.
let local = server spawn --name local --config {
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
			for (let i = 0; i < 16; i++) {
				console.log(`log line ${i}`);
				await tg.sleep(0.250);
			}
		}
	'
}

# Run the process.
let process = tg --url $local.url run --detach $path --remote

# Wait for the process to finish.
let output = tg --url $local.url process wait $process | complete
success $output
snapshot $output.stdout '
	{"exit":0,"output":null}

'

# Restart the remote server.
let remote = server restart $remote

# Ensure we can check the health.
let health = tg --url $remote.url health | complete
success $health

# Get the output.
let output = tg --url $local.url log --no-timeout $process | complete
success $output
snapshot $output.stdout '
	log line 0
	log line 1
	log line 2
	log line 3
	log line 4
	log line 5
	log line 6
	log line 7
	log line 8
	log line 9
	log line 10
	log line 11
	log line 12
	log line 13
	log line 14
	log line 15

'
