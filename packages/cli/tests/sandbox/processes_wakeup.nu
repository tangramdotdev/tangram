use ../lib/test.nu *

# A status notification ends an idle listing without waiting for the polling interval.
let owner = server spawn --name owner --config {
	advanced: { checkpoints: true },
	control: { read_timeout: 60 },
	roles: [api indexer scheduler],
	sandbox: { processes_wakeup_interval: 3600 },
}
let created = tg --url $owner.url runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { url: $owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let sandbox = tg --url $owner.url sandbox create | str trim
tg --url $owner.url index
let watch = tg --url $owner.url checkpoint watch sandbox.get.index | from json | get watch
let socket = $owner.url | str replace 'http+unix://' '' | url decode
let reader = job spawn {
	let job_id = job id
	let output = http get --raw --max-time 15sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/processes'
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $owner.url checkpoint wait sandbox.get.index $watch 0 | ignore
tg --url $owner.url checkpoint unwatch sandbox.get.index $watch
tg --url $runner.url sandbox destroy $sandbox
let output = job recv --tag $reader --timeout 10sec
assert ($output | str contains 'event: end')
