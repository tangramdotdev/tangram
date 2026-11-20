use ../../util/test.nu *

skip_test --cloud

# A process that is queued in a sandbox, whose spawner's server is lost before the sandbox's holder receives it, finishes failed when a user stops or destroys the sandbox, because it can never start.

for command in [stop destroy] {
	# Start a cloud instance with a server that schedules and indexes, a server that only serves the API and spawns the process, and a runner attached to the first.
	let root_token = random chars
	let instance = instance --cloud --config {
		advanced: { checkpoints: true },
		authentication: { root: { token: $root_token } },
	}
	let remote = server spawn --instance $instance --name $'remote-($command)' --config { roles: [api indexer scheduler] }
	let spawner = server spawn --instance $instance --name $'spawner-($command)' --config { roles: [api] }
	let created = tg --url $remote.url --token $root_token runner create | from json
	let runner = server spawn --name $'runner-($command)' --config {
		remotes: { default: { token: $created.token.token, url: $remote.url } },
		runner: { id: $created.data.id, remote: "default", token: $created.token.token },
	}
	let path = artifact {
		tangram.ts: '
			export function loop() {
				return tg.command({ args: ["-c", "while :; do :; done"], executable: "/bin/sh", host: tg.host.current });
			}
			export function exit() {
				return tg.command({ args: ["-c", "exit 0"], executable: "/bin/sh", host: tg.host.current });
			}
		',
	}
	let id = checkin push $remote $path --token $root_token
	let loop = tg --url $remote.url --token $root_token build $'($id)#loop' | str trim
	let finish = tg --url $remote.url --token $root_token build $'($id)#exit' | str trim

	# Create a sandbox, and spawn a process that keeps it running.
	let sandbox = tg --url $remote.url --token $root_token sandbox create --wait --no-tokens | referent node
	tg --url $remote.url --token $root_token spawn --no-tokens $'--sandbox=($sandbox)' $loop | ignore

	# Spawn a second process in the sandbox through the spawner, and hold its request to the sandbox's holder.
	let request = checkpoint watch sandbox.control.request --params { kind: spawn_process } --token $root_token --url $spawner.url
	let url = $spawner.url
	job spawn {
		tg --url $url --token $root_token spawn --no-tokens $'--sandbox=($sandbox)' $finish | complete | ignore
	}
	checkpoint wait $request 0 | ignore
	tg --url $remote.url --token $root_token index
	let queued = tg --url $remote.url --token $root_token ps --status queued | from json
	assert equal ($queued | length) 1 'the process should be queued'
	let process = $queued | first | get node

	# Kill the spawner's server, and end the sandbox.
	let pid = open ($spawner.directory | path join 'lock') | into int
	kill --signal 9 $pid
	wait_for_exit $pid
	tg --url $remote.url --token $root_token sandbox $command --wait $sandbox

	# The queued process finishes failed.
	tg --url $remote.url --token $root_token index
	let data = tg --url $remote.url --token $root_token process get $process | from json
	assert equal $data.status finished "the queued process should finish when the sandbox ends"
	assert equal $data.exit 1
	server stop $runner
	server stop $remote
}
