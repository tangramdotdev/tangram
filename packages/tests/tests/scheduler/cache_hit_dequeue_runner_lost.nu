use ../../util/test.nu *

# A cache hit dequeues its unused candidate sandbox. If the candidate was offered to a runner that is lost before it answers, the candidate is destroyed and does not stay starting.

let root_token = random chars
let cache = server spawn --name cache
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	remotes: { default: { url: $cache.url } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}
let url = $remote.url

let path = artifact {
	tangram.ts: '
		export default () => "cached";
	',
}

# Cache the process at the cache server.
let id = checkin push $cache $path
checkin push $remote $path --token $root_token | ignore
tg --url $cache.url build $id | ignore

# Hold the runner before it accepts an offer, and build the cached process, so that its candidate sandbox is offered and not answered.
let create = checkpoint watch runner.control.create_sandbox --url $runner.url
let build = job spawn {
	let job = job id
	timeout 30s tg --url $url --token $root_token build $id | complete | job send --tag $job 0
}
let candidate = checkpoint wait $create | get params.sandbox
success (job recv --tag $build --timeout 30sec) "the cache hit should win"

# Lose the runner before it answers the offer.
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid

# The candidate is destroyed once its runner expires.
mut status = ''
for _ in 0..40 {
	$status = (tg --url $url --token $root_token sandbox get $candidate | from json | get data.status)
	if $status == destroyed { break }
	sleep 500ms
}
assert equal $status destroyed "the unused candidate should be destroyed"
