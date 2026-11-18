use ../../util/test.nu *

# Making an existing tag public requires admin on it, so a write user cannot make it public.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json
let config = mktemp
{} | to json | save -f $config

let path = artifact { 'child.txt': 'private' }
let directory = tg --token $alice.token checkin --no-tokens $path | referent node
tg --token $alice.token tag put shared $directory
tg --token $alice.token grant $eve.user.id write shared
tg --token $alice.token grant $eve.user.id subtree $directory
tg --token $alice.token index

# Eve has write, so she can put the tag again.
tg --token $eve.token tag put shared $directory

# Eve does not have admin, so she cannot make the tag public.
let output = tg --token $eve.token tag put --public shared $directory | complete
failure $output "a user without admin should not be able to make a tag public"
tg --token $alice.token index
let output = with-env { TANGRAM_CONFIG: $config } {
	tg --url $local.url tag get shared | complete
}
failure $output "the tag should remain private"

# Alice has admin, so she can make the tag public.
tg --token $alice.token tag put --public shared $directory
tg --token $alice.token index
with-env { TANGRAM_CONFIG: $config } {
	assert equal (tg --url $local.url tag get shared | from json | get target.id) $directory
}
