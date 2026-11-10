use ../../util/test.nu *

# An enqueue that the scheduler handed to a runner before it crashed and before it replied is sent again after the response timeout, and the restarted scheduler replies without placing the sandbox again.

let root = random chars
let instance = instance --cloud --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root } },
	scheduler: { request: { response_ttl: 1 } },
}
let scheduler = server spawn --instance $instance --name scheduler --config { roles: [api indexer scheduler] }
let api = server spawn --instance $instance --name api --config { roles: [api] }
let created = tg --url $scheduler.url --token $root runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $scheduler.url } },
	runner: {
		id: $created.data.id,
		remote: "default",
		sandbox_pool_size: 0,
		sandboxes: 2,
		scheduler_ttl: 1,
		token: $created.token.token,
	},
}
wait_for_log $runner 'the runner is ready to accept work'

# Hold the scheduler's index write and its replies, so that the runner accepts the sandbox while the reply waits.
let persist = checkpoint watch scheduler.sandbox.persist --token $root --url $scheduler.url
let reply = checkpoint watch scheduler.response.publish --token $root --url $scheduler.url
let url = $api.url
let create = job spawn {
	let id = job id
	tg --url $url --token $root sandbox create --no-tokens | complete | job send --tag $id 0
}
checkpoint wait $reply | ignore

# Crash the scheduler while the reply is held, and start it again.
kill --signal 9 $scheduler.pid
wait_for_exit $scheduler.pid
let scheduler = server start $scheduler

# The create finishes once the restarted scheduler receives the enqueue again.
let output = job recv --tag $create --timeout 30sec
success $output 'the create should finish after the scheduler restarts'
let sandbox = $output.stdout | referent node

# The sandbox starts on the runner that the scheduler handed it to.
loop {
	let status = tg --url $api.url --token $root sandbox get $sandbox | from json | get data.status
	if $status == 'started' { break }
	assert equal $status 'starting'
	sleep 100ms
}

# The sandbox stays started, because the runner does not boot it a second time.
sleep 3sec
assert equal (tg --url $api.url --token $root sandbox get $sandbox | from json | get data.status) started 'the sandbox should stay started'
