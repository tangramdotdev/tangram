use ../lib/test.nu *

# A successful Start must remain recoverable when the remote crashes before indexing it.
let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
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
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let run_watch = tg --url $runner.url checkpoint watch runner.process.start | from json | get watch
let path = artifact { tangram.ts: 'export default async () => tg.run(await tg.file({ contents: "#!/bin/sh\nprintf 42", executable: true })).sandbox(true);' }
let spawned = tg --url $local.url build --remote --detach --verbose --user $alice.user.id $path | from json
let parent = $spawned.process | split row '?' | first
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.start $run_watch 0 | complete)
tg --url $remote.url --token $root_token index
let start_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.start.received | from json | get watch
let reply_watch = tg --url $runner.url checkpoint watch runner.process.control.start.succeeded | from json | get watch
tg --url $runner.url checkpoint continue runner.process.start $run_watch 0
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.start.received $start_watch 0 | complete)
let write_watch = tg --url $remote.url --token $root_token checkpoint watch index.batch | from json | get watch
tg --url $remote.url --token $root_token checkpoint unwatch process.control.start.received $start_watch
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait index.batch $write_watch 0 | complete)
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.start.succeeded $reply_watch 0 | complete)
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.start $run_watch 1 | complete)

# Kill rather than gracefully drain the queued write.
let pid = open --raw ($remote.directory | path join lock) | str trim | into int
kill --signal 9 $pid
if $nu.os-info.name == "linux" {
	^tail --pid $pid -f /dev/null
} else {
	while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
}
let remote = server start $remote
tg --url $runner.url checkpoint unwatch runner.process.control.start.succeeded $reply_watch
tg --url $runner.url checkpoint unwatch runner.process.start $run_watch
let output = timeout 60s tg --url $local.url wait --remote $parent | complete
success $output "the process must recover its acknowledged initialization after a crash"
assert equal ($output.stdout | from json | get exit) 0 $output.stdout
