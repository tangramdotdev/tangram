use ../../util/test.nu *

# A list includes a node that is visible to the client when the authorization search for a read token exhausts.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	verification: {
		permissions: {
			final: {
				ancestor: { max_depth: 0, max_edges: 0, max_nodes: 0 }
				descendant: { max_depth: 0, max_edges: 0, max_nodes: 0 }
				subtree: { max_objects: 0 }
			}
		}
	}
}
let alice = tg login --verbose --name alice | from json
let config = mktemp
{} | to json | save -f $config
let file = tg --token $alice.token put --no-tokens 'tg.file("public")' | referent node

# Alice puts a public tag in a group.
tg --token $alice.token group create team
tg --token $alice.token tag put --public team/1.0.0 $file
tg --token $alice.token index

# An anonymous client lists the group and its public tag. The list excludes users, because the search for Alice's user, which is not visible to the client, exhausts.
with-env { TANGRAM_CONFIG: $config } {
	let output = tg --url $local.url list --no-users | complete
	success $output "the list should not fail because the search for a read token exhausted"
	assert equal ($output.stdout | from json | get specifier) [team]
	assert equal (tg --url $local.url list team | from json | get specifier) [team/1.0.0]

	# A list that includes Alice's user fails, because the client may neither see nor read it until the search finishes.
	let output = tg --url $local.url list | complete
	failure $output "the list should fail when the search for a node that is not visible exhausts"
	assert ($output.stderr | str contains 'the authorization search exhausted')
}
