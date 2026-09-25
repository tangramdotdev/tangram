use ../lib/test.nu *

for remote_runner in [false true] {
	let owner = server spawn --name owner --config {
		advanced: { checkpoints: true },
		control: { read_timeout: 0.25 },
		roles: (if $remote_runner { [api indexer scheduler] } else { [api indexer runner scheduler] }),
		runner: { sandbox_state_ttl: 60 },
	}
	let runner = if $remote_runner {
		let created = tg --url $owner.url runner create | from json
		server spawn --name runner --config {
			remotes: { default: { url: $owner.url } },
			roles: [api indexer runner],
			runner: { id: $created.data.id, remote: default, sandbox_state_ttl: 60, token: $created.token.token },
		}
	} else { $owner }
	let client = server spawn --name client --config {
		control: { read_timeout: 0.25 },
		remotes: { default: { url: $owner.url } },
	}
	let sandbox = tg --url $owner.url sandbox create | str trim
	tg --url $owner.url index
	for source in [auto runner index] {
		assert equal (tg --url $client.url sandbox get --source $source $sandbox | from json | get data.status) started
	}

	if $remote_runner {
		# An unfinished remote index entry must route status observation to its owner.
		tg --url $runner.url index
		let socket = $runner.url | str replace 'http+unix://' '' | url decode
		let output = http get --allow-errors --full --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/status?source=index&timeout=0&location=local'
		assert equal $output.status 404
		let status = http get --raw --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/status?source=index&timeout=0'
		assert ($status | str contains 'data: "started"')
	}

	# Retained runner completion takes precedence over the unfinished owner index.
	let watch = tg --url $owner.url checkpoint watch sandbox.control.destroy | from json | get watch
	tg --url $owner.url sandbox destroy $sandbox
	timeout 10s tg --url $owner.url checkpoint wait sandbox.control.destroy $watch 0 | ignore
	assert equal (tg --url $runner.url sandbox get --source=runner $sandbox | from json | get data.status) destroyed
	assert equal (tg --url $runner.url sandbox get $sandbox | from json | get data.status) destroyed
	assert equal (tg --url $owner.url sandbox get --source=index $sandbox | from json | get data.status) started
	# Status and wait preserve the same source choice throughout observation.
	let socket = $owner.url | str replace 'http+unix://' '' | url decode
	let status = http get --max-time 10sec --raw --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/status?source=index&timeout=0'
	assert ($status | str contains 'data: "started"')
	assert ($status | str contains 'event: end')
	assert equal (timeout 10s tg --url $runner.url sandbox wait --source=runner $sandbox | from json) destroyed
	let index = tg --url $owner.url checkpoint watch sandbox.get.index | from json | get watch
	let waiter = job spawn {
		let job_id = job id
		let output = timeout 20s tg --url $client.url sandbox wait --source=index $sandbox | complete
		$output | job send --tag $job_id 0
	}
	timeout 10s tg --url $owner.url checkpoint wait sandbox.get.index $index 0 | ignore
	tg --url $owner.url checkpoint unwatch sandbox.get.index $index
	assert equal (try { job recv --tag $waiter --timeout 300ms } catch { null }) null
	tg --url $owner.url checkpoint unwatch sandbox.control.destroy $watch
	let waited = job recv --tag $waiter --timeout 20sec
	success $waited
	assert equal ($waited.stdout | from json) destroyed
	timeout 10s tg --url $owner.url sandbox wait $sandbox | ignore
	tg --url $owner.url index
	assert equal (tg --url $client.url sandbox get --source=index $sandbox | from json | get data.status) destroyed

	# Explicit runner reads cannot be answered by indexed or cached completion.
	let runner = server restart $runner
	let output = timeout 10s tg --url $client.url sandbox get --source=runner $sandbox | complete
	failure $output
	assert ($output.exit_code != 124)
	let output = timeout 10s tg --url $client.url sandbox wait --source=runner $sandbox | complete
	failure $output
	assert ($output.exit_code != 124)
	assert equal (tg --url $client.url sandbox get --source=index $sandbox | from json | get data.status) destroyed
}
