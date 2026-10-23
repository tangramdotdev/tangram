use ../lib/test.nu *

# Cleaned processes disappear after local cleaning or remote runner state expiry.

for location in [local remote] {
	let root_token = random chars
	let local_owner = server spawn --name $'owner-($location)' --now '2026-01-01T00:00:00Z' --config {
		advanced: { checkpoints: true, multi_server: true },
		authentication: { root: { token: $root_token } },
		control: { read_timeout: 0.25 },
		process: { time_to_live: 2 },
		sandbox: { time_to_live: 2 },
		roles: (if $location == local { [api indexer runner scheduler] } else { [api indexer scheduler] }),
		runner: { process_state_ttl: 0 },
	}
	let runner = if $location == remote {
		let created = tg --url $local_owner.url --token $root_token runner create | from json
		server spawn --name runner --now '2026-01-01T00:00:00Z' --config {
			advanced: { checkpoints: true },
			authentication: { root: { token: $root_token } },
			control: { read_timeout: 0.25 },
			remotes: { default: { token: $root_token, url: $local_owner.url } },
			roles: [api indexer runner],
			runner: { id: $created.data.id, process_state_ttl: 0, remote: default, token: $created.token.token },
		}
	} else {
		$local_owner
	}
	let finish_watch = tg --url $runner.url --token $root_token checkpoint watch runner.process.finish | from json | get watch
	let retention_watch = tg --url $runner.url --token $root_token checkpoint watch runner.process.control.retention.finished | from json | get watch
	let path = artifact { tangram.ts: 'export default () => "done";' }
	let id = checkin push $local_owner $path --token $root_token
	let process = tg --url $local_owner.url --token $root_token build --no-tokens --detach $id | referent node
	success (tg --url $runner.url --token $root_token checkpoint wait --timeout 30s runner.process.finish $finish_watch 0 | complete) "the runner must reach the finish checkpoint"
	let params = { process: $process } | to json --raw
	let delete_watch = tg --url $local_owner.url --token $root_token checkpoint watch cleaning.process.delete --params $params | from json | get watch
	tg --url $runner.url --token $root_token checkpoint unwatch runner.process.finish $finish_watch

	# Hold the runner state after completion has been published, then advance the clocks past the time to live and wait for cleaning.
	success (tg --url $runner.url --token $root_token checkpoint wait --timeout 30s runner.process.control.retention.finished $retention_watch 0 | complete) "the runner must finish retention"
	advance_time $local_owner 3sec
	if $location == remote {
		advance_time $runner 3sec
	}
	success (tg --url $local_owner.url --token $root_token checkpoint wait --timeout 30s cleaning.process.delete $delete_watch 0 | complete) "cleaning must delete the process"
	let owner_socket = $local_owner.url | str replace 'http+unix://' '' | url decode
	let socket = $runner.url | str replace 'http+unix://' '' | url decode
	let headers = { Authorization: $'Bearer ($root_token)' }
	if $location == remote {
		let removed = checkpoint watch runner.process.state.removed --url $runner.url --token $root_token --params { process: $process }
		tg --url $runner.url --token $root_token checkpoint unwatch runner.process.control.retention.finished $retention_watch
		checkpoint pass $removed
		checkpoint unwatch $removed
		let response = http get --allow-errors --full --max-time 10sec --unix-socket $socket --headers $headers $'http://localhost/processes/($process)?location=remote'
		assert equal $response.status 404 "the remote runner state should expire"
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

	tg --url $local_owner.url --token $root_token checkpoint unwatch cleaning.process.delete $delete_watch
	if $location == local {
		tg --url $runner.url --token $root_token checkpoint unwatch runner.process.control.retention.finished $retention_watch
	}
}
