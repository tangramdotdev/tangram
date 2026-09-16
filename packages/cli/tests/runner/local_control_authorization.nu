use ../../test.nu *

# A local node capability must not hide parent authority on the owner or in a local grant.
const connect_helper = path self '../lib/process_connect.mjs'
let remote_root = random chars
let runner_root = random chars
let remote = server spawn --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $remote_root }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $remote_root runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $runner_root }, users: { providers: { insecure: true } } },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let reader = tg --url $runner.url login --verbose --name reader | from json
tg --url $runner.url --token $reader.token remote put default $remote.url
let remote_socket = $remote.url | str replace 'http+unix://' '' | url decode
let socket = $runner.url | str replace 'http+unix://' '' | url decode
let script = '#!/bin/sh
trap "" TERM
printf "ready\n"
read line
printf "%s\n" "$line"
printf "%s\n" "$line" >&2
read line || true
'
let value = ['tg.file({ "contents": tg.blob(' ($script | to json --raw) '), "executable": true })'] | str join
let file = tg --url $remote.url --token $remote_root put $value | str trim

for authority in [remote local] {
	for protocol in [connect standalone] {
		let watch = tg --url $runner.url --token $runner_root checkpoint watch runner.process.state.inserted | from json | get watch
		let arg = { command: { node: { executable: { node: { artifact: $file } } } }, sandbox: {}, stdin: pipe, stdout: pipe, stderr: pipe } | to json --raw
		let spawn = job spawn {
			let job_id = job id
			let events = http post --raw --max-time 30sec --unix-socket $remote_socket --headers { Authorization: $'Bearer ($remote_root)', 'Content-Type': 'application/json' } 'http://localhost/processes/spawn' $arg
			let output = $events | lines | where { $in starts-with 'data: ' } | last | str substring 6.. | from json
			$output | job send --tag $job_id 0
		}
		timeout 30s tg --url $runner.url --token $runner_root checkpoint wait runner.process.state.inserted $watch 0 | ignore
		tg --url $runner.url --token $runner_root checkpoint unwatch runner.process.state.inserted $watch
		let spawned = job recv --tag $spawn --timeout 30sec
		let process = $spawned.process
		let capability = $spawned.tokens.local.authorization.0 | split row '.' | get 1 | decode base64 | decode utf-8 | from json
		assert equal $capability.resource $process
		assert equal $capability.permissions [process_parent]
		let response = http get --unix-socket $socket --headers { Authorization: $'Bearer ($runner_root)' } $'http://localhost/processes/($process)?location=remote'
		let node = $response.tokens.local.authorization.0
		let tokens = { local: { authorization: [$node] } }
		let query = $'location=remote&tokens[local][authorization][0]=($node | url encode --all)'
		let reference = $'($process)?($query)'

		# Node authority alone must not permit signaling or accessing pipes.
		if $protocol == standalone {
			let denied = timeout 10s tg --url $runner.url --token $reader.token process signal $reference --signal TERM | complete
			failure $denied
			assert ($denied.exit_code != 124) "an unauthorized signal must fail, not block"
			let denied = "hello\n" | timeout 10s tg --url $runner.url --token $reader.token process stdio write $reference --stream stdin | complete
			failure $denied
			assert ($denied.exit_code != 124) "an unauthorized write must fail, not block"
			for stream in [stdout stderr] {
				let output = timeout 10s tg --url $runner.url --token $reader.token process stdio read $reference --stream $stream --length 6 --no-timeout | complete
				failure $output
				assert ($output.exit_code != 124) "an unauthorized read must fail, not block"
			}
		} else {
			let options = { authorization: $reader.token, tokens: $tokens } | to json --raw
			success (node $connect_helper $socket $process none control_denied remote $options | complete)
		}

		# Parent authority selects the local channel only when it is locally valid.
		let tokens = if $authority == remote {
			$tokens | insert remote $spawned.tokens.local
		} else {
			tg --url $runner.url --token $runner_root grant $reader.user.id process_parent $process
			tg --url $runner.url --token $runner_root index
			$tokens
		}
		if $protocol == standalone {
			let query = if $authority == remote {
				$'($query)&tokens[remote][authorization][0]=($tokens.remote.authorization.0 | url encode --all)'
			} else { $query }
			let reference = $'($process)?($query)'
			let ready = timeout 10s tg --url $runner.url --token $reader.token process stdio read $reference --stream stdout --length 6 --no-timeout | complete
			success $ready
			assert equal $ready.stdout "ready\n"
			success (timeout 10s tg --url $runner.url --token $reader.token process signal $reference --signal TERM | complete)
			let reads = [stdout stderr] | each { |stream|
				let position = if $stream == stdout { 6 } else { 0 }
				let job = job spawn {
					let job_id = job id
					let output = timeout 10s tg --url $runner.url --token $reader.token process stdio read $reference --stream $stream --position $position --length 6 --no-timeout | complete
					$output | job send --tag $job_id 0
				}
				{ job: $job, stream: $stream }
			}
			success ("hello\n" | timeout 10s tg --url $runner.url --token $reader.token process stdio write $reference --stream stdin | complete)
			for read in $reads {
				let output = job recv --tag $read.job --timeout 15sec
				success $output
				assert equal ($output | get $read.stream) "hello\n"
			}
		} else {
			let options = { authorization: $reader.token, tokens: $tokens } | to json --raw
			success (node $connect_helper $socket $process none control remote $options | complete)
		}
		tg --url $remote.url --token $remote_root cancel $process $spawned.lease
	}
}
