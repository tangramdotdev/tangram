use ../../test.nu *

# Reproduces a race condition between two nodes, one acting as [scheduler, http] and another as [cleaner, indexer], and a runner that treats their instance as a trusted remote.
#
# When a parent process spawns a sandbox child via the shortcut path, the runner attempts to checkout under the parent process's session and authorizes teh command artifact before the child's Subtree grant has been written to the outbox. This fails with 'failed to find the artifact.' 

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
	object: { outbox: { partition_total: 1 } }
}

# A separate indexer drains the outbox.
let indexer_directory = mktemp -d
let indexer = server spawn --instance $instance --region a --preserve-keys --name indexer --directory $indexer_directory --config {
	advanced: { single_process: false }
	roles: [cleaner indexer]
	object: { outbox: { partition_total: 1 } }
	indexer: { partition_start: 0, partition_end: 1 }
}

# The runner: a distinct instance with a TRUSTED remote, as the cloud deploys it.
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } }
	roles: [cleaner indexer runner]
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
