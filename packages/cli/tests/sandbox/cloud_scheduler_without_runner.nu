use ../lib/test.nu *

# Sandbox creation completes when a second cloud scheduler joins without any registered runners.

skip_if_no_cloud
let root_token = random chars
let instance = instance --cloud --config {
	authentication: { root: { token: $root_token } },
}
let api = server spawn --instance $instance --preserve-keys --name api --config {
	roles: [api indexer scheduler],
}
let created = tg --url $api.url --token $root_token runner create | from json
# Give the runner enough capacity for every concurrent request.
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $api.url } },
	roles: [api indexer runner],
	runner: {
		cpus: 8,
		id: $created.data.id,
		memory: 8_589_934_592,
		remote: default,
		token: $created.token.token,
	},
}
wait_until { open --raw $runner.log | str contains 'the runner is ready to accept work' } 'the runner must register before the second scheduler starts'
let scheduler = server spawn --instance $instance --name scheduler-without-runner --config {
	roles: [scheduler],
}

# Concurrent requests exercise distribution across the shared NATS queue.
let jobs = 1..8 | each {
	job spawn {
		let id = job id
		let output = timeout 10s tg --url $api.url --token $root_token sandbox create | complete
		$output | job send --tag $id 0
	}
}
let outputs = $jobs | each { |id| job recv --tag $id --timeout 15sec }
for output in $outputs {
	assert ($output.exit_code != 124) 'a scheduler without runners must not strand sandbox creation'
	success $output 'sandbox creation must complete even when another scheduler has no runners'
	tg --url $api.url --token $root_token sandbox destroy ($output.stdout | str trim)
}
