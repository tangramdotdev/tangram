use ../../test.nu *

# Force retargets a tag in place and preserves its grants.

let server = spawn --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let old_target = tg --token $alice.token put 'tg.file("old")' | str trim
tg --token $alice.token tag put shared $old_target
let tag = tg --token $alice.token tag get shared | from json
tg --token $alice.token grant $bob.user.id write shared | ignore
tg index

let new_target = tg --token $bob.token put 'tg.file("new")' | str trim
tg --token $bob.token tag put --force shared $new_target

let updated = tg --token $bob.token tag get shared | from json
assert equal $updated.id $tag.id
assert equal $updated.target.id $new_target
