use ../lib/test.nu *

# A reconnect resumes outcome sync after Finish has already been acknowledged.
let root_token = random chars
let remote = server spawn --name remote --preserve-keys --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let watch = tg --url $runner.url checkpoint watch runner.process.outcome.sync.started | from json | get watch
let finished = tg --url $runner.url checkpoint watch runner.process.control.finish.succeeded | from json | get watch
let synced = tg --url $runner.url checkpoint watch runner.process.outcome.sync.finished | from json | get watch
let alice = tg --url $remote.url login --verbose --name alice | from json
let path = artifact { tangram.ts: 'export default () => tg.file("reconnected outcome");' }
let process = tg --url $remote.url --token $alice.token build --detach --no-tokens $path | referent node
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.outcome.sync.started $watch 0 | complete)
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.finish.succeeded $finished 0 | complete)
tg --url $runner.url checkpoint unwatch runner.process.control.finish.succeeded $finished
let outcome = timeout 30s tg --url $remote.url --token $alice.token wait --source=index $process | from json
assert equal $outcome.exit 0
assert ($outcome.output.value | str contains 'tokens[') "the indexed outcome must carry authorization before reconnect"

let remote_pid = open ($remote.directory | path join 'lock') | into int
kill --signal 9 $remote_pid
wait_until { ps | where pid == $remote_pid | is-empty } "the server must exit"
let remote = server start $remote
tg --url $runner.url checkpoint unwatch runner.process.outcome.sync.started $watch

success (timeout 60s tg --url $runner.url checkpoint wait runner.process.outcome.sync.finished $synced 0 | complete) "the outcome sync must complete after reconnect"
tg --url $runner.url checkpoint unwatch runner.process.outcome.sync.finished $synced

# The server must recover the outcome IDs from its index without another Finish.
let output = timeout 60s tg --url $remote.url --token $alice.token read $outcome.output.value | complete
success $output "the outcome must remain readable after reconnect"
assert equal ($output.stdout | str trim) 'reconnected outcome'
