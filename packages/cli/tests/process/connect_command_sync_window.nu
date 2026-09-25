use ../lib/test.nu *

const script = path self ../lib/process_connect_window.mjs

# Command sync must progress while the entire fixed process request window is buffered.
for await_push in [true false] {
	let root_token = random chars

	# Spawn a scheduler with checkpoints enabled and no local runner.
	let store = { object_max_batch: 1 }
	let remote = server spawn --preserve-keys --name remote --config {
		advanced: { checkpoints: true, single_process: false },
		authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
		roles: [api indexer scheduler],
		process: { await_push: $await_push },
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
		advanced: { checkpoints: true },
		remotes: { default: { token: $alice.token, url: $remote.url } },

	}

	# Store an executable command only on the local server.
	let architecture = (^uname -m | str trim | str replace arm64 aarch64)
	let operating_system = if $nu.os-info.name == 'macos' { 'darwin' } else { $nu.os-info.name }
	let host = $"($architecture)-($operating_system)"
	let contents = "#!/bin/sh\nwhile read -r line; do :; done\n"
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


	let ready = mktemp -d | path join ready
	let socket = $local.url | str replace 'http+unix://' '' | url decode
	let run_watch = tg --url $runner.url checkpoint watch runner.process.run | from json | get watch
	let watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blob } | to json --raw) | from json | get watch
	let run = job spawn {
	 let job_id = job id
	 let output = node $script $socket $command $ready | complete
	 $output | job send --tag $job_id 0
	}
	success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $watch 0 | complete)
	wait_until { $ready | path exists }
	tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $watch
	tg --url $runner.url checkpoint unwatch runner.process.run $run_watch
	let output = job recv --tag $run --timeout 30sec
	success $output "the full request window and stdin EOF must not block command sync"
	assert equal ($output.stdout | str trim) 'ok'

}
