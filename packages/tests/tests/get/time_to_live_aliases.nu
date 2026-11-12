use ../../util/test.nu *

# Local tag reads accept every spelling of the remote cache lifetime flags.
let local = server spawn
let id = tg put --no-tokens 'tg.file("content")' | referent node
tg tag -p foo/a $id

for flag in [--time-to-live --ttl] {
	assert equal (tg get --no-tokens $flag 1h foo/a | from json | get target.id) $id
	assert equal (tg tag get $flag 1h foo/a | from json | get target.id) $id
	assert equal (tg list --no-groups $flag 1h foo | from json | length) 1
}

for flag in [--no-time-to-live --no-ttl] {
	assert equal (tg get --no-tokens $flag foo/a | from json | get target.id) $id
	assert equal (tg tag get $flag foo/a | from json | get target.id) $id
	assert equal (tg list --no-groups $flag foo | from json | length) 1
}
