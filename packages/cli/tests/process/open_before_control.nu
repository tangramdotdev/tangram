use ../lib/test.nu *

# The runner of a scheduled process opens the stdio streams that the connect declared before its process control connection returns, because the assignment carries the opens.

let root_token = random chars

# Spawn a scheduler with checkpoints enabled and no local runner.
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn a separate runner without warm process control connections, so that only the process's own connection reaches the scheduler.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: {
		id: $created.data.id,
		process_control_connection_pool_size: 0,
		remote: 'default',
		token: $created.token.token,
	},
}

# Store an executable command on the scheduler as a user.
let alice = tg --url $remote.url login --verbose --name alice | from json
let architecture = (^uname -m | str trim | str replace arm64 aarch64)
let operating_system = if $nu.os-info.name == 'macos' { 'darwin' } else { $nu.os-info.name }
let host = $"($architecture)-($operating_system)"
let contents = "#!/bin/sh\necho hello\n"
let blob_value = ['tg.blob(' ($contents | to json) ')'] | str join
let blob = tg --url $remote.url --token $alice.token put --no-tokens $blob_value | referent node
let file_value = ['tg.file({"contents":' $blob ',"executable":true})'] | str join
let file = tg --url $remote.url --token $alice.token put --no-tokens $file_value | referent node
let value = ['tg.command({"executable":{"artifact":' $file '},"host":' ($host | to json) '})'] | str join
let command = tg --url $remote.url --token $alice.token put --no-tokens $value | referent node | str trim

# Hold the process control header, and run the process with its stdout read by the client.
let header_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.header | from json | get watch
let write_watch = tg --url $remote.url --token $root_token checkpoint watch process.stdio.write.response | from json | get watch
let claimed_watch = tg --url $remote.url --token $root_token checkpoint watch process.stdio.read.open.claimed | from json | get watch
let run = job spawn {
	let job_id = job id
	tg --url $remote.url --token $alice.token run --cached=false --no-tty --sandbox --stdin null $command | complete | job send --tag $job_id 0
}
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.header $header_watch 0 | complete) "the runner should connect process control"

# The client's read claims the open, and the runner writes the process's stdout to it, while its process control connection is held.
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.stdio.read.open.claimed $claimed_watch 0 | complete) "the client's read should claim the open"
tg --url $remote.url --token $root_token checkpoint unwatch process.stdio.read.open.claimed $claimed_watch
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.stdio.write.response $write_watch 0 | complete) "the runner should open stdout before process control connects"
tg --url $remote.url --token $root_token checkpoint unwatch process.stdio.write.response $write_watch

# Release the header, and the run completes with its output.
tg --url $remote.url --token $root_token checkpoint unwatch process.control.header $header_watch
let output = job recv --tag $run --timeout 30sec
success $output "the run should complete"
assert ($output.stdout | str contains 'hello') "the process should produce its output"
