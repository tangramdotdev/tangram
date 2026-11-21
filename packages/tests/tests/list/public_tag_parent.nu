use ../../util/test.nu *

# A public tag makes its ancestor groups appear in an anonymous client's list, whether the tag is made public by a grant or when it is put, and whether or not the put creates the groups.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let config = mktemp
{} | to json | save -f $config
let file = tg --token $alice.token put --no-tokens 'tg.file("public")' | referent node

# Alice puts a private tag and then grants public read on it.
tg --token $alice.token group create granted
tg --token $alice.token tag put granted/1.0.0 $file
tg --token $alice.token grant public read granted/1.0.0

# Alice puts a public tag.
tg --token $alice.token group create put
tg --token $alice.token tag put --public put/1.0.0 $file

# Alice puts two public tags that create their ancestors.
tg --token $alice.token tag put --public --create-ancestors created/nested/1.0.0 $file
tg --token $alice.token tag put --public --create-ancestors created/nested/2.0.0 $file
tg --token $alice.token index

with-env { TANGRAM_CONFIG: $config } {
	# An anonymous client gets both tags.
	assert equal (tg --url $local.url tag get granted/1.0.0 | from json | get target.id) $file
	assert equal (tg --url $local.url tag get put/1.0.0 | from json | get target.id) $file

	# An anonymous client lists both parent groups.
	let specifiers = tg --url $local.url list | from json | get specifier
	assert ('granted' in $specifiers) "the parent of a tag granted to the public should be listed"
	assert ('put' in $specifiers) "the parent of a tag put publicly should be listed"

	# An anonymous client gets the tags that created their ancestors and lists the ancestor at the root.
	assert equal (tg --url $local.url tag get created/nested/1.0.0 | from json | get target.id) $file
	assert equal (tg --url $local.url tag get created/nested/2.0.0 | from json | get target.id) $file
	assert ('created' in $specifiers) "the created ancestor of a tag put publicly should be listed"

	# An anonymous client lists the created ancestors down to the tags.
	assert equal (tg --url $local.url list created | from json | get specifier) [created/nested]
	assert equal (tg --url $local.url list created/nested | from json | get specifier | sort) [created/nested/1.0.0 created/nested/2.0.0]
}
