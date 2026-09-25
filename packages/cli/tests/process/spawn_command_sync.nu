use ../lib/test.nu *

# Direct spawn honors push ordering for command IDs and inline command objects.
for location in [remote 'local(east)'] {
	for await_push in [true false] {
		for inline in [false true] {
			let root_token = random chars
			let instance = instance --primary-region east --regions [{ name: east } { name: west }]

			# Spawn a scheduler with checkpoints enabled and no local runner.
			let store = { object_max_batch: 1 }
			let remote = server spawn --preserve-keys --instance $instance --region east --url (instance region url $instance east) --name remote --config {
				advanced: { checkpoints: true, single_process: false },
				authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
				roles: [api indexer scheduler],
				sync: { get: { store: { lmdb: $store, memory: $store, scylla: $store } } },
			}
			let created = tg --url $remote.url --token $root_token runner create | from json

			# Spawn a separate runner with checkpoints enabled.
			let runner = server spawn --name runner --config {
				advanced: { checkpoints: true },
				remotes: { default: { token: $created.token.token, url: $remote.url } },
				roles: [api indexer runner],
				runner: { id: $created.data.id, remote: 'default', token: $created.token.token },

			}

			# Create a user and a local server that routes runs through the scheduler.
			let alice = tg --url $remote.url login --verbose --name alice | from json
			let config = {
				advanced: { checkpoints: true },
				authentication: { root: { token: $root_token } },
				remotes: { default: { token: $alice.token, url: $remote.url } },
				process: { await_push: $await_push },
			}
			let local = if $location == 'remote' {
				server spawn --name local --config $config
			} else {
				server spawn --instance $instance --region west --directory (mktemp -d) --name local --config $config
			}

			# Store an executable command only on the local server.
			let architecture = (^uname -m | str trim | str replace arm64 aarch64)
			let operating_system = if $nu.os-info.name == 'macos' { 'darwin' } else { $nu.os-info.name }
			let host = $"($architecture)-($operating_system)"
			let contents = "#!/bin/sh\necho hello\n"
			let blob_value = ['tg.blob(' ($contents | to json) ')'] | str join
			let blob = tg --url $local.url put $blob_value | str trim
			let file_value = ['tg.file({"contents":' $blob ',"executable":true})'] | str join
			let file = tg --url $local.url put $file_value | str trim
			let value = (
				['tg.command({"executable":{"artifact":' $file '},"host":' ($host | to json) '})']
				| str join
			)
			let command = (
				tg --url $local.url put $value
				| str trim
			)

			# Hold the executable blob so the scheduler receives the command before its full graph.
			let store_watch = (
				tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blob } | to json --raw)
				| from json
				| get watch
			)
			let state_watch = (
				tg --url $runner.url checkpoint watch runner.process.state.inserted
				| from json
				| get watch
			)

			# Hold execution to isolate spawn ordering from concurrent artifact checkout.
			let run_watch = tg --url $runner.url checkpoint watch runner.process.run | from json | get watch
			let push_watch = tg --url $local.url checkpoint watch process.spawn.command.push.finished | from json | get watch
			let socket = $local.url | str replace 'http+unix://' '' | url decode
			let command = if $inline {
				{ node: { executable: { node: { artifact: $file } }, host: $host } }
			} else {
				$command
			}
			let arg = {
				cached: false,
				command: $command,
				location: $location,
				sandbox: {},
				stderr: 'null',
				stdin: 'null',
				stdout: 'null',
			} | to json --raw
			let spawn = job spawn {
				let job_id = job id
				let output = http post --raw --max-time 30sec --unix-socket $socket --headers { 'Content-Type': 'application/json', Authorization: $'Bearer ($root_token)' } 'http://localhost/processes/spawn' $arg
				$output | job send --tag $job_id 0
			}
			success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $store_watch 0 | complete) "the push should reach the held command object"
			if $await_push {
				failure (timeout 1s tg --url $runner.url checkpoint wait runner.process.state.inserted $state_watch 0 | complete) "the default must push before spawning"
				let output = try { job recv --tag $spawn --timeout 1sec } catch { null }
				assert equal $output null
				tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $store_watch
			}
			success (timeout 30s tg --url $runner.url checkpoint wait runner.process.state.inserted $state_watch 0 | complete) "the process should be scheduled"
			tg --url $runner.url checkpoint unwatch runner.process.state.inserted $state_watch
			let events = job recv --tag $spawn --timeout 30sec
			assert (not ($events | str contains 'event: error')) "the direct spawn request should succeed"
			let output = $events | lines | where { $in starts-with 'data: ' } | last | str substring 6.. | from json
			if not $await_push {
				# The direct spawn client has exited while the command graph is still in flight.
				tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $store_watch
				success (timeout 30s tg --url $local.url checkpoint wait process.spawn.command.push.finished $push_watch 0 | complete) "the command push should complete after the spawn response"
			}
			tg --url $local.url checkpoint unwatch process.spawn.command.push.finished $push_watch
			wait_until { (tg --url $remote.url --token $root_token get $blob | complete).exit_code == 0 }
			tg --url $runner.url checkpoint unwatch runner.process.run $run_watch
			let result = tg --url $remote.url --token $root_token process wait $output.process | from json
			assert equal $result.exit 0 ($result | to json --raw)
		}
	}
}
