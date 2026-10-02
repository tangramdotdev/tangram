use ../lib/test.nu *

# A shortcut child's cache lookup on the remote must not fail while its shortcut parent's start is still pending.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let root_token = random chars

# Spawn the remote and create the runner.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner_config = {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { cpus: 3, id: $created.data.id, remote: default, token: $created.token.token },
}
let runner = server spawn --name runner --config $runner_config

let path = artifact {
	tangram.ts: '
		export async function first() {
			return await tg.build(parent, "first").sandbox();
		}

		export async function second() {
			return await tg.build(parent, "second").sandbox();
		}

		export async function parent(name: string) {
			return await tg.build(child).cached(name === "second").sandbox();
		}

		export function child() {
			return 42;
		}
	',
}

# Build the child once so the remote can reuse it.
let output = timeout 60s tg --url $remote.url --token $root_token build $"($path)#first" | complete
success $output "the first build should succeed"
assert equal ($output.stdout | str trim) '42'

# Wait for the cache entries to be indexed before stopping the runner.
tg --url $runner.url index
tg --url $remote.url --token $root_token index

# Replace the runner so the child is only cached on the remote.
server stop $runner
let runner = server spawn --name runner-fresh --config $runner_config

# Hold the parent's command push so the remote does not start the parent.
let push_watch = tg --url $runner.url checkpoint watch runner.process.command.push.started | from json | get watch
let spawned = tg --url $remote.url --token $root_token build --detach --verbose $"($path)#second" | from json
let grandparent = $spawned.process | split row '?' | first
success (timeout 60s tg --url $runner.url checkpoint wait runner.process.command.push.started $push_watch 0 | complete) "the parent should reach its command push"

# Keep the parent's start pending while the child attempts its required cache lookup.
let pushed = timeout 30s tg --url $runner.url checkpoint wait runner.process.command.push.started $push_watch 1 | complete
if $pushed.exit_code == 0 {
	tg --url $runner.url checkpoint continue runner.process.command.push.started $push_watch 1
}

# Release the parent's push.
tg --url $runner.url checkpoint continue runner.process.command.push.started $push_watch 0
tg --url $runner.url checkpoint unwatch runner.process.command.push.started $push_watch

let output = timeout 60s tg --url $remote.url --token $root_token wait $grandparent | complete
success $output "the second build should finish"
let wait = $output.stdout | from json
assert equal $wait.exit 0 "the second build should succeed"
assert equal $wait.output 42

# The remote should record the child under the parent.
let parents = tg --url $remote.url --token $root_token process children --no-tokens $grandparent | from json
assert equal ($parents | length) 1
let parent = $parents | get 0.process | split row '?' | first
let children = tg --url $remote.url --token $root_token process children --no-tokens $parent | from json
assert equal ($children | length) 1
assert (($children | get 0 | get -o cached) == true) "the child should be a cache hit"
