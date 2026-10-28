use ../lib/test.nu *

# A parent finishes without waiting for its child's start to reach the home instance, and the home instance then finds the child instead of reporting that it does not exist.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	control: { read_timeout: 60 },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
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
		}
		export async function child() {
			await tg.sleep(60);
		}
	'
}

# Hold the child's start at the home instance, and let the parent finish.
let received = tg --url $remote.url --token $root_token checkpoint watch process.control.start.received | from json | get watch
let parent = tg --url $local.url run --no-tokens --detach $path --remote | referent node
let child = tg --url $remote.url --token $root_token checkpoint wait --timeout 30s process.control.start.received $received 0 | from json | get params.process
let output = timeout 30s tg --url $remote.url --token $alice.token process wait $parent | complete
success $output "the parent must finish while its child's start is held"
tg --url $remote.url --token $root_token index

# The home instance lists the child in the finished parent's children, and a lookup of the child must not report that it does not exist.
let children = tg --url $remote.url --token $alice.token process children --no-tokens $parent | from json | get process
assert ($children | any { |entry| $entry | str starts-with $child }) "the finished parent must list the child"
let reader = job spawn {
	let job_id = job id
	let output = timeout 30s tg --url $remote.url --token $root_token process get --no-tokens $child | complete
	$output | job send --tag $job_id 0
}
assert equal (try { job recv --tag $reader --timeout 300ms } catch { null }) null "the lookup must wait for the child's start"

# Release the start, and the lookup must find the child.
tg --url $remote.url --token $root_token checkpoint continue process.control.start.received $received 0
tg --url $remote.url --token $root_token checkpoint unwatch process.control.start.received $received
let output = job recv --tag $reader --timeout 30sec
success $output "the home instance must find the child"
