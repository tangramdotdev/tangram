use ../../util/test.nu *

# A later lazy push reports the processes the remote already stores as skipped.

let remote = server spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg --url $remote.url login --verbose --name alice | from json

let source = server spawn --name source --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let later = server spawn --name later --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice builds a parent process that calls a child, and pushes the whole tree to the remote.
let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build(child)
		}

		export function child() {
			return tg.file("hello")
		}
	'
}
let parent = tg --url $source.url build --no-tokens --detach $path | referent node
tg --url $source.url wait $parent
tg --url $source.url index
tg --url $source.url push $parent --process-children --process-command-objects --process-log-objects
tg --url $remote.url index

# A later server has only the parent process node.
tg --url $source.url get --no-tokens $parent | tg --url $later.url put --no-tokens --id $parent | referent node
tg --url $later.url index

# The remote already stores both processes, so the push reports them as skipped.
let output = tg --url $later.url --no-quiet push --lazy $parent --process-children --no-process-output-objects --no-process-error-objects | complete
success $output "The later lazy push should succeed."
assert ($output.stderr | str contains 'skipped 2 processes') "The push should report both processes as skipped."
