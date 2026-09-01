use ../../test.nu *

# A sandboxed child checkout must use a process-aware pull when local authorization is insufficient.
# The first run stores the child command and its executable on the runner. The checkpoints order the second run so that only Node access to the command has been established locally before the child starts.

let root_token = random chars
let remote = server spawn --cloud --name remote --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	roles: [api indexer scheduler]
}

let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true }
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } }
	roles: [api indexer runner]
	runner: { id: $created.data.id, remote: "default", token: $created.token.token }
}

let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } }
}

let path = artifact {
	"example.tg.ts": '
		export default () => {
			return tg.run(child).sandbox(true);
		};

		export const child = () => undefined;
	'
}

let module = $"($path)/example.tg.ts"

# Run once and wait for the child command grant to be indexed.
let initial_index_watch = (
	tg --url $runner.url checkpoint watch index.batch.finished --params '{"command_object_grant":true}'
	| from json
	| get watch
)
let initial_run = job spawn {
	let job_id = job id
	let output = tg --url $local.url run --cached=false --no-tty --remote --user $alice.user.id $module | complete
	$output | job send --tag $job_id 0
}
let output = timeout 30s tg --url $runner.url checkpoint wait index.batch.finished $initial_index_watch 0 | complete
success $output "the initial child command grant should finish indexing"
tg --url $runner.url checkpoint continue index.batch.finished $initial_index_watch 0
tg --url $runner.url checkpoint unwatch index.batch.finished $initial_index_watch
let output = try { job recv --tag $initial_run --timeout 30sec } catch { null }
if $output == null {
	error make { msg: "the initial run did not complete" }
}
success $output "the initial process should succeed"

# Hold the reused child command until its grant is indexed.
let command_push_watch = (
	tg --url $runner.url checkpoint watch runner.process.command.push.finished
	| from json
	| get watch
)
let index_batch_watch = (
	tg --url $runner.url checkpoint watch index.batch.finished --params '{"command_object_grant":true}'
	| from json
	| get watch
)
let run = job spawn {
	let job_id = job id
	let output = tg --url $local.url run --cached=false --no-tty --remote --user $alice.user.id $module | complete
	$output | job send --tag $job_id 0
}
let output = timeout 30s tg --url $runner.url checkpoint wait index.batch.finished $index_batch_watch 0 | complete
success $output "the child command grant should finish indexing"
tg --url $runner.url checkpoint continue index.batch.finished $index_batch_watch 0
tg --url $runner.url checkpoint unwatch index.batch.finished $index_batch_watch
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.command.push.finished $command_push_watch 0 | complete
success $output "the child command push should finish"
tg --url $runner.url checkpoint continue runner.process.command.push.finished $command_push_watch 0
tg --url $runner.url checkpoint unwatch runner.process.command.push.finished $command_push_watch

let output = try { job recv --tag $run --timeout 30sec } catch { null }
if $output == null {
	error make { msg: "the run did not complete after the checkpoints continued" }
}
success $output "the sandboxed child process should succeed after its command grant is indexed"
