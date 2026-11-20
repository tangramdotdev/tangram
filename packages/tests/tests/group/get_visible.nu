use ../../util/test.nu *

# A client that can read a tag in a group, but not the group, can get the group and list it, and sees nothing else in it.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let eve = tg login --verbose --name eve | from json
let config = mktemp
{} | to json | save -f $config
let file = tg --token $alice.token put --no-tokens 'tg.file("public")' | referent node

# Alice creates a group with a member, a public tag, and a private tag.
let group = tg --token $alice.token group create team | from json
tg --token $alice.token group members add team $bob.user.id
tg --token $alice.token tag put --public team/1.0.0 $file
tg --token $alice.token tag put team/2.0.0 $file
tg --token $alice.token index

def check [url: string, token?: string] {
	let flags = if $token == null { [] } else { [--token $token] }

	# The client gets the group.
	assert equal (tg --url $url ...$flags group get team | from json | get id) $group.id
	assert equal (tg --url $url ...$flags get team | from json | get id) $group.id

	# The client lists only the public tag.
	let items = tg --url $url ...$flags list team | from json
	assert equal ($items | get specifier) [team/1.0.0]

	# The client does not get the private tag.
	let output = tg --url $url ...$flags tag get team/2.0.0 | complete
	failure $output "the private tag should not be readable"

	# The client does not list the members or the grants.
	let output = tg --url $url ...$flags group members list team | complete
	failure $output "the members should not be listable"
	let output = tg --url $url ...$flags grants list --resource team | complete
	failure $output "the grants should not be listable"

	# The tokens that the client receives for the group do not let it list the members or get the private tag.
	let root = tg --url $url ...$flags list | from json | where specifier == team | get 0
	let tokens = $root.node.options
		| get --optional tokens.local
		| default []
	for token in $tokens {
		let options = $'tokens[local][0]=($token | url encode --all)'
		let output = tg --url $url ...$flags group members list $'team?($options)' | complete
		failure $output "a token for a visible group should not list its members"
		let output = tg --url $url ...$flags tag get $'team/2.0.0?($options)' | complete
		failure $output "a token for a visible group should not read its private tag"
	}
}

# An anonymous client sees only the public tag.
with-env { TANGRAM_CONFIG: $config } { check $local.url }

# A user without a grant sees only the public tag.
check $local.url $eve.token
