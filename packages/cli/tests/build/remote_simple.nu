use ../../test.nu *

# Start the remote server.
let config = {
	runner: false,
}
let remote = spawn -n remote --cloud --config $config

# Start the runner server.
let config = {
	remotes: [
		{
			name: "default",
			url: $remote.url
		}
	],
	runner: {
		remotes: ["default"],
	}
}
let runner = spawn -n runner --config $config

# Start the local server.
let config = {
	remotes: [
		{
			name: "default",
			url: $remote.url
		}
	]
}
let local = spawn -n local --config $config

let path = artifact {
	tangram.ts: '
		export default () => tg.build(child);
		export const child = () => 42;
	'
};

# Run a remote build
let id = tg build --remote -d $path
let output = tg wait $id | from json
assert ($output.exit == 0)
snapshot $output.output '42'
