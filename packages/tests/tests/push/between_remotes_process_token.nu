use ../../util/test.nu *

# Pushing a process referent to one remote gets the process, its child, and their objects from another remote with the referent's tokens for that remote, under both eager and lazy push.

def test [...args] {
	# Spawn remote A with authentication and remote B.
	let a = server spawn --cloud --name a --config { authentication: { users: { providers: { insecure: true } } } }
	let b = server spawn --cloud --name b

	# Alice builds a process that calls a child and pushes the whole tree privately to remote A.
	let alice = tg --url $a.url login --verbose --name alice | from json
	let alice_local = server spawn --name alice-local --config {
		remotes: { default: { token: $alice.token, url: $a.url } },
	}
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
	let parent = tg --url $alice_local.url build --no-tokens --detach $path | referent node
	tg --url $alice_local.url wait $parent
	tg --url $alice_local.url index
	let child = tg --url $alice_local.url process children $parent | from json | get 0 | get process | referent node
	let output = tg --url $alice_local.url push $parent --process-children --process-command-objects --process-log-objects
	let tokens = $output | referent tokens remote
	tg --url $a.url index

	# Spawn a local server that uses remote A as Bob.
	let bob = tg --url $a.url login --verbose --name bob | from json
	let local = server spawn --name local --config {
		remotes: {
			a: { token: $bob.token, url: $a.url },
			b: { url: $b.url },
		}
	}

	# Confirm that pushing the process without the tokens fails.
	let output = tg --url $local.url push $parent --remote=b --process-children --process-command-objects --process-log-objects ...$args | complete
	failure $output "should fail to push the process without the tokens"

	# Push the process to remote B with Alice's tokens for remote A.
	let query = $tokens | enumerate | each {|token| $'tokens[remote:a][($token.index)]=($token.item | url encode --all)' } | str join '&'
	let referent = $'($parent)?($query)'
	let output = tg --url $local.url push $referent --remote=b --process-children --process-command-objects --process-log-objects ...$args | complete
	success $output "should push the process to remote B"

	# Confirm the process and its child are on remote B and match remote A.
	for process in [$parent $child] {
		let a_process = tg --url $a.url --token $alice.token process get --no-tokens $process | from json
		let b_process = tg --url $b.url process get --no-tokens $process | from json
		assert equal $b_process.command $a_process.command
		assert equal $b_process.output $a_process.output
	}
}

test "--eager"
test "--lazy"
