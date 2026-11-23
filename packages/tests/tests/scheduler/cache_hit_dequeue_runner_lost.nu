use ../../util/test.nu *

# A cache hit dequeues its unused candidate sandbox. If the candidate was offered to a runner that is lost before it answers, the candidate is destroyed and does not stay starting.

let root_token = random chars
let cache = server spawn --name cache
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
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

# Hold the runner before it accepts an offer, hold the remote's cache lookup, and build the cached process.
let offer = checkpoint watch runner.control.offer_boot --url $runner.url
let enqueued = checkpoint watch scheduler.sandbox.enqueued --token $root_token --url $url
let cached = checkpoint watch process.spawn.cached.region_or_remote --token $root_token --url $url
let build = job spawn {
	let job = job id
	timeout 30s tg --url $url --token $root_token build $id | complete | job send --tag $job 0
}

# Wait for the candidate sandbox to be enqueued and offered to the runner, and then let the cache lookup continue, so that the cache hit wins and cancels the candidate while its offer is unanswered.
checkpoint pass $enqueued | ignore
checkpoint unwatch $enqueued
let candidate = checkpoint wait $offer | get params.sandbox
checkpoint pass $cached | ignore
checkpoint unwatch $cached

# Lose the runner before it answers the offer.
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid

# Advance the remote's clock past the runner ttl, so that the scheduler removes the runner.
let removed = checkpoint watch scheduler.runner.removed --params { runner: $created.data.id } --token $root_token --url $url
advance_time $remote 4sec
checkpoint pass $removed | ignore
checkpoint unwatch $removed

# The build succeeds with the cache hit, and the candidate is destroyed.
success (job recv --tag $build --timeout 30sec) "the cache hit should win"
tg --url $url --token $root_token index
assert equal (tg --url $url --token $root_token sandbox get $candidate | from json | get data.status) destroyed "the unused candidate should be destroyed"
