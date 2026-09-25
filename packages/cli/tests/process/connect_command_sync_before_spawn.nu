use ../lib/test.nu *

# Both run and spawn await the command transfer before scheduling by default.
for mode in [run spawn] {
	let root_token = random chars

	# Spawn a scheduler with checkpoints enabled and no local runner.
	let store = { object_max_batch: 1 }
	let remote = server spawn --preserve-keys --name remote --config {
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
	let local = server spawn --name local --config {
		remotes: { default: { token: $alice.token, url: $remote.url } },

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

	let run = job spawn {
		let job_id = job id
		let output = if $mode == 'spawn' {
			tg --url $local.url process spawn --cached=false --sandbox --no-tty --remote --user $alice.user.id $command | complete
		} else {
			tg --url $local.url run --cached=false --no-tty --remote --user $alice.user.id $command | complete
		}
		$output | job send --tag $job_id 0
	}
	success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $store_watch 0 | complete) "the command transfer should reach the executable blob"
	failure (timeout 1s tg --url $runner.url checkpoint wait runner.process.state.inserted $state_watch 0 | complete) "the process must not be scheduled before the command transfer completes"
	let output = try { job recv --tag $run --timeout 1sec } catch { null }
	assert equal $output null "spawn must not return before the command transfer completes"

	# Release the command transfer and verify that spawning follows.
	tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $store_watch
	success (timeout 30s tg --url $runner.url checkpoint wait runner.process.state.inserted $state_watch 0 | complete) "the process should be scheduled after the command transfer"
	tg --url $runner.url checkpoint unwatch runner.process.state.inserted $state_watch
	let output = job recv --tag $run --timeout 30sec
	success $output
	if $mode == 'spawn' {
		let process = $output.stdout | str trim
		let output = tg --url $local.url process wait $process | from json
		assert equal $output.exit 0
	} else {
		assert ($output.stdout | str contains 'hello')
	}
}
