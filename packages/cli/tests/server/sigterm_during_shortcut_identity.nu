use ../lib/test.nu *

# SIGTERM cleans up a claimed physical sandbox even when a shortcut has not completed its sandbox or process control handshake.

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
		runner: { cpus: 1, overcommit: { cpu: 1 }, id: $created.data.id, remote: default, sandbox_pool_size: 1, token: $created.token.token },
	}
	let alice = tg --url $remote.url login --verbose --name alice | from json
	let local = server spawn --name $'local-($kind)' --config {
		remotes: { default: { token: $alice.token, url: $remote.url } },
	}
	# A shortcut sandbox's connection is the only one that carries a proof, and a shortcut process connects process control after the parent's connection.
	let checkpoint = $'runner.($kind).control.connect'
	let control_watch = if $kind == 'sandbox' {
		let params = { shortcut: 'true' } | to json --raw
		tg --url $runner.url checkpoint watch $checkpoint --params $params | from json | get watch
	} else {
		tg --url $runner.url checkpoint watch $checkpoint | from json | get watch
	}
	let control_hit = if $kind == 'sandbox' { 0 } else { 1 }
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
	if $kind == 'process' {
		success (timeout 30s tg --url $runner.url checkpoint wait $checkpoint $control_watch 0 | complete)
		tg --url $runner.url checkpoint continue $checkpoint $control_watch 0
	}
	let claimed = timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.pool.take $pool_watch 1 | from json
	# Track the physical sandbox by its launcher PID independently of its command arguments.
	let sandbox_pid = $claimed.params.pid | into int
	assert (ps | any { |process| $process.pid == $sandbox_pid }) "the claimed physical sandbox should be running"
	tg --url $runner.url checkpoint unwatch runner.sandbox.pool.take $pool_watch
	success (timeout 30s tg --url $runner.url checkpoint wait $checkpoint $control_watch $control_hit | complete)

	# Leave the handshake blocked and terminate the runner.
	let pid = open ($runner.directory | path join lock) | into int
	kill --signal 15 $pid
	wait_for_exit --timeout 20sec $pid "the runner should exit without waiting for the shortcut handshake"
	wait_for_exit --timeout 10sec $sandbox_pid "the child's physical sandbox should exit with the runner"
	job kill $build
	server stop $local
	server stop $remote
}
