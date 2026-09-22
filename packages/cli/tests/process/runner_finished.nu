use ../lib/test.nu *

# Cleaned processes disappear after local cleaning or remote runner state expiry.

for location in [local remote] {
	let root_token = random chars
	let owner = server spawn --name $'owner-($location)' --config {
		advanced: { checkpoints: true, single_process: false },
		authentication: { root: { token: $root_token } },
		indexer: { cleaning: {} },
		process: { time_to_live: 2 },
		roles: (if $location == local { [api indexer runner scheduler] } else { [api indexer scheduler] }),
		runner: { process_state_ttl: 0 },
	}
	let runner = if $location == remote {
		let created = tg --url $owner.url --token $root_token runner create | from json
		server spawn --name runner --config {
			advanced: { checkpoints: true },
			authentication: { root: { token: $root_token } },
			remotes: { default: { token: $root_token, url: $owner.url } },
			roles: [api indexer runner],
			runner: { id: $created.data.id, process_state_ttl: 0, remote: default, token: $created.token.token },
		}
	} else {
		$owner
	}
	let finish_watch = tg --url $runner.url --token $root_token checkpoint watch runner.process.finish | from json | get watch
	let retention_watch = tg --url $runner.url --token $root_token checkpoint watch runner.process.control.retention.finished | from json | get watch
	let path = artifact { tangram.ts: 'export default () => "done";' }
	let process = tg --url $owner.url --token $root_token build --detach $path | str trim
	success (timeout 30s tg --url $runner.url --token $root_token checkpoint wait runner.process.finish $finish_watch 0 | complete) "the runner must reach the finish checkpoint"
	let params = { process: $process } | to json --raw
	let delete_watch = tg --url $owner.url --token $root_token checkpoint watch cleaning.process.delete --params $params | from json | get watch
	tg --url $runner.url --token $root_token checkpoint unwatch runner.process.finish $finish_watch

	# Hold the runner state after completion has been published, then wait for index expiry.
	success (timeout 30s tg --url $runner.url --token $root_token checkpoint wait runner.process.control.retention.finished $retention_watch 0 | complete) "the runner must finish retention"
	success (timeout 30s tg --url $owner.url --token $root_token checkpoint wait cleaning.process.delete $delete_watch 0 | complete) "cleaning must delete the process"
	let owner_socket = $owner.url | str replace 'http+unix://' '' | url decode
	let socket = $runner.url | str replace 'http+unix://' '' | url decode
	let headers = { Authorization: $'Bearer ($root_token)' }
	if $location == remote {
		tg --url $runner.url --token $root_token checkpoint unwatch runner.process.control.retention.finished $retention_watch
		wait_until {
			let response = http get --allow-errors --full --max-time 10sec --unix-socket $socket --headers $headers $'http://localhost/processes/($process)?location=remote'
			$response.status == 404
		} --timeout 10sec "the remote runner state should expire"
	}
	let response = http get --allow-errors --full --max-time 10sec --unix-socket $owner_socket --headers $headers $'http://localhost/processes/($process)?location=local'
	assert equal $response.status 404

	# Root authorization remains valid, but the index entry and runner state are gone.
	for endpoint in ['' '/status' '/children'] {
		let response = http get --allow-errors --full --max-time 10sec --unix-socket $socket --headers $headers $'http://localhost/processes/($process)($endpoint)?location=($location)&timeout=0'
		assert equal $response.status 404 $'($location) process ($endpoint) must not return the cleaned process'
	}
	let response = http post --allow-errors --full --max-time 10sec --unix-socket $socket --headers ($headers | insert Accept 'text/event-stream') $'http://localhost/processes/($process)/wait?location=($location)' ''
	assert equal $response.status 404 $'($location) process wait must not return the cleaned process'

	tg --url $owner.url --token $root_token checkpoint unwatch cleaning.process.delete $delete_watch
	if $location == local {
		tg --url $runner.url --token $root_token checkpoint unwatch runner.process.control.retention.finished $retention_watch
	}
}
