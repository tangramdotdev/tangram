use ../../test.nu *

# `foo` and `bar` are exports that both `tg.build(shared)` but have different commands.
# `foo` runs on one runner and pushes its output to the remote
# `bar` runs on a second runner and pushes its output to the remote.
#
# all builds initiated by the same user must succeed
#
let root_token = random chars

# Spawn a remote.
let remote = spawn --name remote --cloud --preserve-keys --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [cleaner http indexer scheduler],
}

let created = tg --url $remote.url --token $root_token runner create | from json
let runner_config = {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.runner.id, remote: 'default', token: $created.token.token },
}

# Alice is an ordinary authenticated user driving her own server.
let alice = tg --url $remote.url login --verbose alice | from json
let local = spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: '
		export function shared() {
			return tg.file("cached");
		}
		export function foo() {
			return tg.build(shared);
		}
		export function bar() {
			return tg.directory({ reused: tg.build(shared) });
		}
	'
}

# start runner 1
let runner1 = spawn --name runner1 --config $runner_config

# run foo, guaranteeing that it lands on runner1.
let foo = tg --url $local.url build --remote $"($path)#foo" | complete
success $foo "the foo build must populate the remote's process cache."

# Kill runner1 to force runner 2 to pick up the next build.
let pid = open ($runner1.directory | path join 'lock') | into int
kill --signal 2 $pid
if $nu.os-info.name == 'linux' {
	^tail --pid $pid -f /dev/null
} else {
	while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
}

# start runner2.
let runner2 = spawn --name runner2 --config $runner_config

# build bar, guaranteeing it lands on runner2.
let bar = tg --url $local.url build --remote $"($path)#bar" | complete
success $bar "the runner must push a parent output containing an output it cache-hit from the remote."

# Verify the user can read the pushed output and the reused child output.
let directory = $bar.stdout | str trim
let output = tg --url $local.url get $directory --depth inf | complete
success $output "the user must read the pushed output."
