use ../lib/test.nu *

# A completion notification restarts the first status/wait observation.
let owner = server spawn --name owner --config {
	advanced: { checkpoints: true },
	control: { read_timeout: 60 },
	process: { status_wakeup_interval: 3600 },
	roles: [api indexer scheduler],
}
let created = tg --url $owner.url runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { url: $owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
for method in [status wait] {
	let finish = tg --url $runner.url checkpoint watch runner.process.finish | from json | get watch
	let path = artifact { tangram.ts: $'export default () => "($method)";' }
	let process = tg --url $owner.url spawn $path | str trim
	timeout 10s tg --url $runner.url checkpoint wait runner.process.finish $finish 0 | ignore
	tg --url $owner.url index
	let watch = tg --url $owner.url checkpoint watch process.get.control | from json | get watch
	let reader = job spawn {
		let job_id = job id
		let output = if $method == status { timeout 10s tg --url $owner.url process status --no-timeout $process | complete } else { timeout 10s tg --url $owner.url wait $process | complete }
		$output | job send --tag $job_id 0
	}
	timeout 10s tg --url $owner.url checkpoint wait process.get.control $watch 0 | ignore
	tg --url $runner.url checkpoint unwatch runner.process.finish $finish
	let output = job recv --tag $reader --timeout 15sec
	success $output
	if $method == status { assert ($output.stdout | str contains 'finished') }
	tg --url $owner.url checkpoint unwatch process.get.control $watch
}
