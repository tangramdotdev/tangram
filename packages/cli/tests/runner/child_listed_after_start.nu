use ../lib/test.nu *

# Through the home instance, a child that a remote runner starts on the shortcut path is listed among its parent's children only once the home instance has received the child's start.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: 'default', token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.build(child).spawn();
			await tg.sleep(60);
		}
		export async function child() {
			await tg.sleep(60);
		}
	'
}

# Hold the child's start at the home instance.
let received = tg --url $remote.url --token $root_token checkpoint watch process.control.start.received | from json | get watch
let succeeded = checkpoint watch runner.process.control.start.succeeded --url $runner.url
let parent = tg --url $local.url run --no-tokens --detach $path --remote | referent node
let child = tg --url $remote.url --token $root_token checkpoint wait --timeout 30s process.control.start.received $received 0 | from json | get params.process

# The home instance must not list the child while its start is held.
let children = tg --url $remote.url --token $alice.token process children --no-tokens $parent | from json | get process
assert ($children | all { |entry| not ($entry | str starts-with $child) }) "the home instance must not list a child whose start it has not received"

# Release the start, and wait for the runner to record that it succeeded.
tg --url $remote.url --token $root_token checkpoint continue process.control.start.received $received 0
tg --url $remote.url --token $root_token checkpoint unwatch process.control.start.received $received
checkpoint pass $succeeded 0 | ignore
checkpoint unwatch $succeeded

# The home instance must now list the child, and find it.
let children = tg --url $remote.url --token $alice.token process children --no-tokens $parent | from json | get process
assert ($children | any { |entry| $entry | str starts-with $child }) "the home instance must list the child after receiving its start"
success (tg --url $remote.url --token $alice.token process get --no-tokens $child | complete) "the home instance must find the child"
