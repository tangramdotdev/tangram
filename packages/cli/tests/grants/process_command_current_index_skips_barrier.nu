use ../../test.nu *

# Spawning and finishing with a command authorized by the current index do not require a barrier.

let directory = mktemp -d
let root_token = random chars
let config = {
	advanced: { single_process: false },
	authentication: {
		root: { token: $root_token },
		users: { providers: { insecure: true } },
	},
}
let server = server spawn --preserve-keys --directory $directory --config $config
let alice = tg --url $server.url login --verbose --name alice | from json
let path = artifact {
	tangram.ts: '
		export function run() {
			return "hello";
		}

		export default function () {
			return tg.command(run);
		}
	',
}
let command = (
	tg --url $server.url --token $alice.token build $path
	| str trim
)
tg --url $server.url --token $alice.token grant $alice.user.id object_subtree $command | ignore
tg --url $server.url --token $alice.token index
let config = (
	$server.config
	| upsert advanced.single_process true
	| upsert authorization.tokens null
	| upsert roles [http runner scheduler]
)

# Restart without an indexer so an attempted barrier fails.
let pid = open ($server.directory | path join 'lock') | into int
kill --signal 2 $pid
if $nu.os-info.name == "linux" {
	^tail --pid $pid -f /dev/null
} else {
	while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
}
let server = server spawn --directory $directory --config $config
failure (tg --url $server.url --token $alice.token index | complete)

let output = (
	tg --url $server.url --token $alice.token run --cached=false --detach --local --stderr null --stdout null --verbose $command
	| complete
)
success $output "a current subtree authorization should avoid an index barrier."
let process = $output.stdout | from json | get process
let output = tg --url $server.url --token $alice.token wait $process | complete
success $output "finishing with a current subtree authorization should avoid an index barrier."
