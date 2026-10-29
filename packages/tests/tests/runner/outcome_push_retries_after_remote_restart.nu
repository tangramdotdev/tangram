use ../../lib/test.nu *

# A runner whose outcome push is interrupted by a remote restart retries the push, so the outcome objects reach the remote.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			return tg.file("outcome");
		}
	'
}

# Hold the outcome push before it starts.
let outcome_watch = tg --url $runner.url checkpoint watch runner.process.outcome.sync.started | from json | get watch
let process = tg --url $local.url run --no-tokens --detach $path --remote | referent node
success (tg --url $runner.url checkpoint wait --timeout 30s runner.process.outcome.sync.started $outcome_watch 0 | complete) "the runner must start the outcome push"

# Stop the remote, release the push so that its first attempt fails, and then start the remote again.
server stop $remote
let finished = checkpoint watch runner.process.outcome.sync.finished --params { process: $process } --url $runner.url
let failure_watch = tg --url $runner.url checkpoint watch push.attempt.failed | from json | get watch
tg --url $runner.url checkpoint unwatch runner.process.outcome.sync.started $outcome_watch
success (tg --url $runner.url checkpoint wait --timeout 30s push.attempt.failed $failure_watch 0 | complete) "the first outcome push attempt must fail"
tg --url $runner.url checkpoint unwatch push.attempt.failed $failure_watch
let remote = server start $remote
success (tg --url $remote.url health | complete)

# Wait for the retried push to finish, which delivers the output to the remote.
checkpoint pass $finished | ignore
checkpoint unwatch $finished
let data = tg --url $remote.url --token $root_token process get $process | from json
let output = $data.output.node.value | split row '?' | first
success (tg --url $remote.url --token $root_token get $output | complete) "the output must reach the remote after the outcome push is retried"
