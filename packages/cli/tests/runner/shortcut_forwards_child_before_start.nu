use ../lib/test.nu *

# A shortcut process that forwards a child spawn to the remote before the remote has started the shortcut process must still own the child's sandbox correctly.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let root_token = random chars

# Spawn the remote and create the runner.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	control: { read_timeout: 60 },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Give the runner room for one sandbox, so the second concurrent grandchild must be forwarded and borrow the shortcut process's lease through the scheduler.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { cpus: 1, id: $created.data.id, remote: default, token: $created.token.token },
}

# Hold Start on the server while the shortcut process runs on the runner.
let start_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.start.received | from json | get watch
let finished_watch = tg --url $runner.url checkpoint watch runner.process.finished | from json | get watch

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build(shortcut).sandbox();
		}

		export async function shortcut() {
			const [a, b] = await Promise.all([
				tg.build(first).sandbox(),
				tg.build(second).sandbox(),
			]);
			return a + b;
		}

		export function first() {
			return 20;
		}

		export function second() {
			return 22;
		}
	',
}
let build = job spawn {
	let job_id = job id
	let output = tg --url $remote.url --token $root_token build $path | complete
	$output | job send --tag $job_id 0
}

# The shortcut process runs and forwards its child spawn while its own start is held.
success (timeout 60s tg --url $remote.url --token $root_token checkpoint wait process.control.start.received $start_watch 0 | complete) "the server should receive Start"

# Let the grandchild that borrows the lease on the runner start if it is ready.
let started = timeout 2s tg --url $remote.url --token $root_token checkpoint wait process.control.start.received $start_watch 1 | complete
if $started.exit_code == 0 {
	tg --url $remote.url --token $root_token checkpoint continue process.control.start.received $start_watch 1
}
let finished = timeout 2s tg --url $runner.url checkpoint wait runner.process.finished $finished_watch 0 | complete
if $finished.exit_code == 0 {
	tg --url $runner.url checkpoint continue runner.process.finished $finished_watch 0
}
tg --url $runner.url checkpoint unwatch runner.process.finished $finished_watch

# Release Start so the server can initialize the shortcut process.
tg --url $remote.url --token $root_token checkpoint continue process.control.start.received $start_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch process.control.start.received $start_watch

let output = job recv --tag $build --timeout 60sec
assert (not ($output.stderr | str contains "invalid owner")) "the forwarded child must not be owned by the shortcut process"
success $output "the forwarded child spawn should succeed"
assert equal ($output.stdout | str trim) '42'
