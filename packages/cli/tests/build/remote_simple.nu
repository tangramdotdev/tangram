use ../../test.nu *

# A remote build executed through a runner completes with a zero exit code and returns the expected output.

# Start the remote server.
let root_token = random chars
let config = {
	authentication: { root: { token: $root_token } },
	roles: [cleaner finalizer http indexer scheduler],
}
let remote = spawn --name remote --cloud --config $config

# Create the runner.
let created = tg --url $remote.url --token $root_token runner create | from json

# Start the runner server.
let config = {
	remotes: {
		default: {
			token: $created.token.token
			url: $remote.url
		}
	},
	runner: {
		id: $created.runner.id
		remote: "default",
		token: $created.token.token
	}
}
let runner = spawn --name runner --config $config

# Start the local server.
let config = {
	remotes: {
		default: {
			token: $root_token
			url: $remote.url
		}
	}
}
let local = spawn --name local --config $config

let path = artifact {
	tangram.ts: '
		export default function () { return tg.build(child); }
		export function child() { return 42; }
	'
};

# Run a remote build
let id = tg build --remote --detach $path
let output = tg wait $id | from json
assert ($output.exit == 0)
snapshot $output.output '42'
