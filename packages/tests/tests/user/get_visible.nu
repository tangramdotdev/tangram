use ../../util/test.nu *

# A client that can read a tag under a user, but not the user, can list the user's public tag and cannot get the user.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let config = mktemp
{} | to json | save -f $config
let file = tg --token $alice.token put --no-tokens 'tg.file("public")' | referent node

# Alice puts a public tag and a private tag under her user.
tg --token $alice.token tag put --public alice/1.0.0 $file
tg --token $alice.token tag put alice/2.0.0 $file
tg --token $alice.token index

# Alice gets her user.
assert equal (tg --token $alice.token user get alice | from json | get id) $alice.user.id

with-env { TANGRAM_CONFIG: $config } {
	# An anonymous client lists only the public tag.
	assert equal (tg --url $local.url list alice | from json | get specifier) [alice/1.0.0]

	# An anonymous client does not get the user, whose data includes its emails.
	let output = tg --url $local.url user get alice | complete
	failure $output "the user should not be readable"

	# An anonymous client does not get the private tag.
	let output = tg --url $local.url tag get alice/2.0.0 | complete
	failure $output "the private tag should not be readable"
}
