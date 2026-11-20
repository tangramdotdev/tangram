use ../../util/test.nu *

# A client that can read a tag in an organization, but not the organization, can get the organization and list it, and sees nothing else in it.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let eve = tg login --verbose --name eve | from json
let config = mktemp
{} | to json | save -f $config
let file = tg --token $alice.token put --no-tokens 'tg.file("public")' | referent node

# Alice creates an organization with a member, a private group, and a group with a public tag and a private tag.
let organization = tg --token $alice.token organization create acme | from json
tg --token $alice.token organization members add acme $bob.user.id
tg --token $alice.token group create acme/secret
tg --token $alice.token group create acme/tool
tg --token $alice.token tag put --public acme/tool/1.0.0 $file
tg --token $alice.token tag put acme/tool/2.0.0 $file
tg --token $alice.token index

def check [url: string, token?: string] {
	let flags = if $token == null { [] } else { [--token $token] }

	# The client gets the organization.
	assert equal (tg --url $url ...$flags organization get acme | from json | get id) $organization.id
	assert equal (tg --url $url ...$flags get acme | from json | get id) $organization.id

	# The client lists only the group with the public tag, and only the public tag in it.
	assert equal (tg --url $url ...$flags list acme | from json | get specifier) [acme/tool]
	assert equal (tg --url $url ...$flags list acme/tool | from json | get specifier) [acme/tool/1.0.0]

	# The client does not get the private group or the private tag.
	let output = tg --url $url ...$flags group get acme/secret | complete
	failure $output "the private group should not be readable"
	let output = tg --url $url ...$flags tag get acme/tool/2.0.0 | complete
	failure $output "the private tag should not be readable"

	# The client does not list the members or the grants.
	let output = tg --url $url ...$flags organization members list acme | complete
	failure $output "the members should not be listable"
	let output = tg --url $url ...$flags grants list --resource acme | complete
	failure $output "the grants should not be listable"

	# The tokens that the client receives for the organization do not let it list the members or get the private tag.
	let root = tg --url $url ...$flags list | from json | where specifier == acme | get 0
	let tokens = $root.node.options
		| get --optional tokens.local
		| default []
	for token in $tokens {
		let options = $'tokens[local][0]=($token | url encode --all)'
		let output = tg --url $url ...$flags organization members list $'acme?($options)' | complete
		failure $output "a token for a visible organization should not list its members"
		let output = tg --url $url ...$flags tag get $'acme/tool/2.0.0?($options)' | complete
		failure $output "a token for a visible organization should not read its private tag"
	}
}

# An anonymous client sees only the public tag.
with-env { TANGRAM_CONFIG: $config } { check $local.url }

# A user without a grant sees only the public tag.
check $local.url $eve.token
