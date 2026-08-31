use ../../test.nu *

# Reproduces a bug when a parent process spawns a sandbox child via the shortcut path.
#
# The child's checkout runs on behalf of a sandbox whose network access is disabled. The checkout's pull used the non-process remote path, which refuses a sandbox origin without network access, so it could not sync the child's command grant from the remote and fell back to the local index, which lacks the grant. This fails with 'failed to find the artifact.'

let root_token = random chars
let common = {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}
let regions = [{ name: 'a' }]
let instance = instance --cloud --primary-region a --regions $regions --config $common

# The server: http + scheduler, single_process:false, one outbox partition.
let remote_directory = mktemp -d
let remote = server spawn --instance $instance --region a --preserve-keys --name remote --directory $remote_directory --url (instance region url $instance a) --config {
	roles: [http scheduler]
	object: { index_outbox: { partition_total: 1 } }
}

# A separate indexer drains the outbox.
let indexer_directory = mktemp -d
let indexer = server spawn --instance $instance --region a --preserve-keys --name indexer --directory $indexer_directory --config {
	advanced: { single_process: false }
	roles: [indexer]
	object: { index_outbox: { partition_total: 1 } }
	indexer: { partitions: { start: 0, end: 1 } }
}

# The runner: a distinct instance with a distinct index, as the cloud deploys it.
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } }
	roles: [indexer runner]
	runner: { id: $created.runner.id, remote: "default", token: $created.token.token }
}

# A user, and a local server that submits the remote run.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config { remotes: { default: { token: $alice.token, url: $remote.url } } }

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

for i in 0..9 {
	let output = tg --url $local.url run --no-tty --remote --user $alice.user.id $"($path)/example.tg.ts" --arg-string $"n($i)" | complete
	success $output $"the nested sandboxed run must not race the child's command grant iteration ($i)"
}
