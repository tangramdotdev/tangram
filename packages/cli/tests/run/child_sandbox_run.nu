use ../../test.nu *

# A remote run that nested a sandboxed run should succeed and capture the output of both processes.

# Create the instance. Its configuration is inherited by the remote and the indexer.
let root_token = random chars
let remote_directory = mktemp -d
let indexer_directory = mktemp -d
let regions = [{ name: 'a' }]
let common = {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}
let instance = instance --cloud --primary-region a --regions $regions --config $common

# Start the remote server.
let remote = server spawn --instance $instance --region a --preserve-keys --name remote --directory $remote_directory --url (instance region url $instance a) --config {
	roles: [http indexer scheduler],
}

# Start a separate indexer server that shares the remote's databases, as the cloud does.
let indexer = server spawn --instance $instance --region a --preserve-keys --name indexer --directory $indexer_directory --config {
	roles: [indexer],
}

# Create the runner.
let created = tg --url $remote.url --token $root_token runner create | from json

# Start the runner server.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.runner.id, remote: "default", token: $created.token.token },
}

# Log in to the remote as a user.
let alice = tg --url $remote.url login --verbose --name alice | from json

# Start the local server.
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let path = artifact {
	"example.tg.ts": '
		export default () => {
			console.log("outer");
			return tg.run(child).sandbox(true);
		};

		export const child = () => {
			console.log("inner");
			return tg.file("hello!");
		};
	'
}

let output = tg --url $local.url run --no-tty --remote --user $alice.user.id $"($path)/example.tg.ts" | complete
success $output "the remote nested sandboxed run should succeed"
assert ($output.stdout | str contains "outer") "the parent process should log to stdout"
assert ($output.stdout | str contains "inner") "the child process should log to stdout"
assert ($output.stdout | str contains "fil_") "the run should return a file id"
