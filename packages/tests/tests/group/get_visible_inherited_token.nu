use ../../util/test.nu *

# A user who reads a group through a grant on its parent receives a read token for the group, whether or not the group is also visible to the user through a public tag.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let file = tg --token $alice.token put --no-tokens 'tg.file("public")' | referent node

# Alice creates a group in a group and grants Bob read on the parent.
tg --token $alice.token group create team
let group = tg --token $alice.token group create team/tool | from json
tg --token $alice.token grant $bob.user.id read team
tg --token $alice.token index

# Bob gets the group with a read token.
let output = with-env { TANGRAM_QUIET: "false" } { tg --token $bob.token group get team/tool | complete }
success $output
assert equal ($output.stdout | from json | get id) $group.id
assert ($output.stderr | str contains '{"local":[') "the group should have a read token"

# Alice puts a public tag in the group.
tg --token $alice.token tag put --public team/tool/1.0.0 $file
tg --token $alice.token index

# Bob still gets the group with a read token.
let output = with-env { TANGRAM_QUIET: "false" } { tg --token $bob.token group get team/tool | complete }
success $output
assert equal ($output.stdout | from json | get id) $group.id
assert ($output.stderr | str contains '{"local":[') "the group should still have a read token"
