use ../../util/test.nu *

# A runner on another server that is interrupted keeps its control stream open while its running process finishes, and only then tells the scheduler that it shut down, so that the scheduler does not end the running process's boot.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	indexer: { id: 'idx_0000000000000000000000000000' },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { heartbeat_interval: 0.1, id: $created.data.id, remote: default, token: $created.token.token },
}

# Hold a FIFO open for writing before the process starts, in a holder process that records its pid, so that the process's read blocks until the test kills the holder.
let directory = mktemp --directory
let release = $directory | path join release
let held = $directory | path join held
mkfifo $release $held
let module = '
	export default async () => {
		await tg.host.readFile(RELEASE);
		return tg.file("late output");
	};
' | str replace 'RELEASE' ($release | to json --raw)
let path = artifact { tangram.ts: $module }
job spawn {
	do --ignore-errors { bash -c 'exec 3<> "$1"; echo $$ > "$2"; exec sleep 3600' _ $release $held }
} | ignore
let holder = open --raw $held | str trim | into int

# Spawn a process on the remote that blocks reading the FIFO in its mount, and wait for it to start.
let id = checkin push $remote $path --token $root_token
let process = tg --url $remote.url --token $root_token spawn --sandbox --mount $'($directory):($directory)' --no-tokens --verbose $id | from json | get process
tg --url $remote.url --token $root_token process status --until started $process | ignore

# Interrupt the runner, and let two of its heartbeats reach the scheduler, so that the test continues past the point where a runner that ended its boot at the interrupt would have done so. The process is still running.
let heartbeat = checkpoint watch runner.control.heartbeat.received --params { runner: $created.data.id } --token $root_token --url $remote.url
kill --signal 2 $runner.pid
checkpoint pass $heartbeat 0 | ignore
checkpoint pass $heartbeat 1 | ignore
checkpoint unwatch $heartbeat
assert equal (tg --url $remote.url --token $root_token process status --timeout 0 $process | from json) [started] "the scheduler should not end the running process's boot"

# Release the process. The runner exits once the process finishes, and the process keeps its real outcome.
^kill -KILL $holder
wait_for_exit --timeout 30sec $runner.pid "the runner should exit after the process finishes"
let outcome = tg --url $remote.url --token $root_token wait $process | from json
assert equal $outcome.exit 0 "the process should finish successfully during the runner's shutdown"
let output = tg --url $remote.url --token $root_token cat $outcome.output.value
assert equal $output 'late output' "the process's output should survive the runner's shutdown"
