use ../../test.nu *

# A remote build executed through a runner completes with a zero exit code and returns the expected output.

# Start the remote server.
let config = {
	runner: false,
}
let remote = spawn --name remote --cloud --config $config

# Start the runner server.
let config = {
	remotes: {
		default: {
			url: $remote.url
		}
	},
	runner: {
		remote: "default",
	}
}
let runner = spawn --name runner --config $config

# Start the local server.
let config = {
	remotes: {
		default: {
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
