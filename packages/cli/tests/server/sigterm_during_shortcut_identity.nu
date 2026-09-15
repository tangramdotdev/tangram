use ../../test.nu *

# SIGTERM cleans up a claimed physical sandbox even when a shortcut has not received its sandbox or process ID.

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
			export default () => tg.build(child).sandbox();
			export function child() { return 0; }
		',
	}
	let build = job spawn { tg --url $local.url build --remote $path | complete | ignore }

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
	let started = timeout 1s tg --url $runner.url checkpoint wait runner.process.start $start_watch 1 | complete
	assert equal $started.exit_code 124 "the shortcut child must not start before receiving its identity"

	# Leave identity acquisition blocked and terminate the runner.
	let pid = open ($runner.directory | path join lock) | into int
	kill --signal 15 $pid
	wait_until --timeout 20sec { ps | where pid == $pid | is-empty } "the runner should exit without waiting for shortcut identity acquisition"
	wait_until --timeout 10sec { ps | where pid in $pids | is-empty } "the unstarted child's physical sandbox should exit with the runner"
	job kill $build
	server stop $local
	server stop $remote
}
