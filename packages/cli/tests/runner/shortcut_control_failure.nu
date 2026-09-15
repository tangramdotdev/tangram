use ../../test.nu *

# A rejected shortcut handshake releases its physical sandbox and borrowed capacity without running the child.

for kind in [sandbox process] {
	let root_token = random chars
	let remote = server spawn --preserve-keys --name $'remote-($kind)' --config {
		advanced: { single_process: false },
		authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
		roles: [api indexer scheduler],
	}
	let created = tg --url $remote.url --token $root_token runner create | from json
	let runner = server spawn --name $'runner-($kind)' --config {
		advanced: { checkpoints: true },
		remotes: { default: { token: $created.token.token, url: $remote.url } },
		roles: [api indexer runner],
		runner: { cpus: 1, id: $created.data.id, remote: default, sandbox_pool_size: 1, token: $created.token.token },
	}
	let alice = tg --url $remote.url login --verbose --name alice | from json
	let local = server spawn --name $'local-($kind)' --config {
		remotes: { default: { token: $alice.token, url: $remote.url } },
	}

	let checkpoint = $'runner.($kind).control.connect'
	let params = { $kind: 'None' } | to json --raw
	let control_watch = tg --url $runner.url checkpoint watch $checkpoint --params $params | from json | get watch
	let pool_watch = tg --url $runner.url checkpoint watch runner.sandbox.pool.take | from json | get watch
	let start_watch = tg --url $runner.url checkpoint watch runner.process.start | from json | get watch
	let path = artifact {
		tangram.ts: '
			export default async function () {
				try {
					return await tg.build(child).sandbox();
				} catch {
					return 42;
				}
			}
			export function child() { return 0; }
		',
	}
	let build = job spawn {
		let job_id = job id
		let output = tg --url $local.url build --remote $path | complete
		$output | job send --tag $job_id 0
	}

	# Let the parent start, then capture the child's physical sandbox before activation.
	success (timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.pool.take $pool_watch 0 | complete)
	tg --url $runner.url checkpoint continue runner.sandbox.pool.take $pool_watch 0
	success (timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 0 | complete)
	tg --url $runner.url checkpoint continue runner.process.start $start_watch 0
	let claimed = timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.pool.take $pool_watch 1 | from json
	let sandbox_path = $claimed.params.path
	let pids = ps --long | where { |process| $process.command | str contains $sandbox_path } | get pid
	assert ($pids | is-not-empty) "the claimed physical sandbox should be running"
	tg --url $runner.url checkpoint unwatch runner.sandbox.pool.take $pool_watch
	success (timeout 30s tg --url $runner.url checkpoint wait $checkpoint $control_watch 0 | complete)

	# Reject only new runner-authenticated connections; the parent's assigned control streams remain valid.
	tg --url $remote.url --token $root_token runner token delete $created.data.id $created.token.id
	tg --url $runner.url checkpoint unwatch $checkpoint $control_watch
	let output = job recv --tag $build --timeout 30sec
	success $output "the parent should catch the rejected shortcut handshake"
	assert equal ($output.stdout | str trim) '42'
	let started = timeout 1s tg --url $runner.url checkpoint wait runner.process.start $start_watch 1 | complete
	assert equal $started.exit_code 124 "a child without its identity must not start"
	tg --url $runner.url checkpoint unwatch runner.process.start $start_watch
	wait_until --timeout 10sec { ps | where pid in $pids | is-empty } "the failed shortcut's physical sandbox should exit"
	wait_until --timeout 10sec { not ($sandbox_path | path exists) } "the failed shortcut's temporary directory should be removed"

	# A scheduled build requires the sole CPU allocation, proving that the failed child released its borrow.
	let followup = artifact { tangram.ts: 'export default () => 43' }
	let output = timeout 30s tg --url $local.url build --remote $followup | complete
	success $output "the runner should have capacity for a subsequent scheduled build"
	assert equal ($output.stdout | str trim) '43'
	server stop $local
	server stop $runner
	server stop $remote
}
