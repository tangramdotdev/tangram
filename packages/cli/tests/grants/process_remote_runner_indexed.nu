use ../../test.nu *

# A remote runner indexes a process it runs and retains the process's grants after finish.

let remote_root_token = random chars
let remote = spawn --name remote --cloud --config {
	authentication: { root: { token: $remote_root_token } },
	roles: [cleaner http indexer scheduler],
}
let created = tg --url $remote.url --token $remote_root_token runner create | from json

let runner_root_token = random chars
let runner = spawn --name runner --config {
	authentication: {
		root: { token: $runner_root_token },
		users: { providers: { insecure: true } },
	},
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.runner.id, remote: default, token: $created.token.token },
}
let local = spawn --name local --config {
	remotes: { default: { token: $remote_root_token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: 'export default function () { return tg.file("hello"); }'
}
let process = tg --url $local.url build --remote --detach $path | str trim
let result = tg --url $local.url wait $process | from json
assert equal $result.exit 0 "the remote process should finish successfully."
let output = $result.output.value | split row '?' | first

# Restart the runner without its remote so all remaining reads must be served locally.
let runner_directory = $runner.directory
let runner_url = $runner.url
let pid = open ($runner.directory | path join lock) | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the runner should stop"
let runner = spawn --name runner --directory $runner_directory --url $runner_url --config {
	authentication: {
		root: { token: $runner_root_token },
		users: { providers: { insecure: true } },
	},
	roles: [http indexer],
}

let indexed = tg --url $runner.url --token $runner_root_token process get --local $process | complete
success $indexed "the runner should index the remote process locally."
let indexed = $indexed.stdout | from json
assert equal $indexed.status finished "the runner should index the finished process data."
assert equal ($indexed.output.value | split row '?' | first) $output "the runner should index the process output relationship."

let user = tg --url $runner.url login --verbose --name user | from json
tg --url $runner.url --token $runner_root_token grant $user.user.id process_parent $process | ignore
let contents = tg --url $runner.url --token $user.token cat $output | complete
success $contents "process_parent should confer the process's durable output grant."
assert equal ($contents.stdout | str trim) hello "the runner should serve the process output locally."
