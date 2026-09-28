use ../lib/test.nu *

# A status notification ends an idle listing without waiting for the polling interval.
let local_owner = server spawn --name local-owner --config {
	advanced: { checkpoints: true },
	control: { read_timeout: 60 },
	roles: [api indexer scheduler],
	sandbox: { processes_wakeup_interval: 3600 },
}
let created = tg --url $local_owner.url runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { url: $local_owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let sandbox = tg --url $local_owner.url sandbox create | str trim
tg --url $local_owner.url index
let watch = tg --url $local_owner.url checkpoint watch sandbox.get.index | from json | get watch
let socket = $local_owner.url | str replace 'http+unix://' '' | url decode
let reader = job spawn {
	let job_id = job id
	let output = http get --raw --max-time 15sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/processes'
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $local_owner.url checkpoint wait sandbox.get.index $watch 0 | ignore
tg --url $local_owner.url checkpoint unwatch sandbox.get.index $watch
tg --url $runner.url sandbox destroy $sandbox
let output = job recv --tag $reader --timeout 10sec
assert ($output | str contains 'event: end')
