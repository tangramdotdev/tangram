use ../../util/test.nu *

# A runner whose scheduler declares it dead while a sandbox is started on it abandons the boot and answers no request for its processes, also after it reconnects, so that a process of the boot shows the failure that the scheduler recorded.

# Get the text of a process's error, which is inline or an error object.
def error_text [url: string, token: string, error: any] {
	if ($error | describe) == 'string' {
		tg --url $url --token $token get $error
	} else {
		$error | to json --raw
	}
}

let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", scheduler_ttl: 1, token: $created.token.token },
}
wait_for_log $runner 'the runner is ready to accept work'
let url = $remote.url

# Create a FIFO to mount, whose read blocks the command until the process is canceled, because nothing writes to it.
let directory = mktemp --directory
let fifo = $directory | path join fifo
mkfifo $fifo
let module = '
	export function block() {
		return tg.command({
			args: ["-ec", "read line < \"$0\"", FIFO],
			executable: "/bin/sh",
			host: tg.host.current,
		});
	}
' | str replace 'FIFO' ($fifo | to json --raw)
let path = artifact { tangram.ts: $module }
let id = checkin push $remote $path --token $root_token
let command = tg --url $url --token $root_token build $'($id)#block' | str trim

# Spawn the command in a new sandbox with a nonzero ttl, and wait for it to start.
let process = tg --url $url --token $root_token spawn --sandbox --ttl 1h --mount $'($directory):($directory)' --no-tokens --verbose $command | from json | get process
let sandbox = tg --url $url --token $root_token process get $process | from json | get sandbox
tg --url $url --token $root_token process status --until started $process | ignore

# Hold the runner's heartbeats, and advance the remote's clock past the runner ttl, so that the scheduler declares the runner dead and ends the boot while the process runs. The runner learns that the boot ended and abandons the boot, which cancels the process, and the runner's finish of the process is held while the process's control stream stays open.
let finish = checkpoint watch runner.process.finish --params { process: $process } --url $runner.url
let abandoned = checkpoint watch runner.boot.abandoned --params { sandbox: $sandbox } --url $runner.url
let heartbeat = checkpoint watch runner.control.heartbeat.received --params { runner: $created.data.id } --token $root_token --url $url
checkpoint wait $heartbeat 0 | ignore
let expired = checkpoint watch scheduler.runner.expired --params { runner: $created.data.id } --token $root_token --url $url
advance_time $remote 4sec
checkpoint pass $expired | ignore
checkpoint unwatch $expired
checkpoint wait $finish | ignore
checkpoint pass $abandoned | ignore
checkpoint unwatch $abandoned

# The process shows the failure that the scheduler recorded, because the runner answers no request for a process of a boot that it abandoned.
let data = tg --url $url --token $root_token process get $process | from json
assert equal $data.status finished "the process should be finished"
assert equal $data.error?.node?.message? "heartbeat expired" "the process should show the scheduler's failure"

# The runner's own server shows the process failed and the sandbox stopped, because the runner records the end of a boot that it abandoned in its server's copies of the remote's records.
let local = tg --url $runner.url --token $root_token process get $process | from json
assert equal $local.status finished "the runner's server should show the process finished"
assert equal $local.exit 1 "the runner's server should show the process failed"
assert (error_text $runner.url $root_token $local.error | str contains "the sandbox's scheduler ended the boot") "the runner's server should show the end of the boot"
assert equal (tg --url $runner.url --token $root_token sandbox get $sandbox | from json | get data.status) stopped "the runner's server should show the sandbox stopped"

# Let the runner stop the sandbox, let its heartbeats continue, and advance the remote's clock past the runner's scheduler ttl, so that the runner connects again.
let stopped = checkpoint watch runner.sandbox.stopped --params { sandbox: $sandbox } --url $runner.url
checkpoint unwatch $finish
checkpoint pass $stopped | ignore
checkpoint unwatch $stopped
let connect = checkpoint watch scheduler.runner.connect --params { runner: $created.data.id } --token $root_token --url $url
checkpoint unwatch $heartbeat
advance_time $remote 2sec
checkpoint pass $connect | ignore
checkpoint unwatch $connect

# The process keeps the failure that the scheduler recorded after indexing.
tg --url $url --token $root_token index
let data = tg --url $url --token $root_token process get $process | from json
assert equal $data.status finished "the process should stay finished"
assert equal $data.error?.node?.message? "heartbeat expired" "the process should keep the scheduler's failure"
assert equal (tg --url $url --token $root_token sandbox get $sandbox | from json | get data.status) stopped "the sandbox should be stopped"
