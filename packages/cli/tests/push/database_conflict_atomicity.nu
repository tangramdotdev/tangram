use ../../test.nu *

# A conflicting database node rejects the complete batch without committing non-conflicting siblings.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}

let local_root = tg --url $local.url group create atomic | from json
tg --url $local.url push atomic
let remote_root = tg --url $remote.url group get atomic | from json
assert equal $remote_root.id $local_root.id

let remote_conflict = tg --url $remote.url group create atomic/conflict | from json
let local_new = tg --url $local.url group create atomic/a-new | from json
let local_conflict = tg --url $local.url group create atomic/conflict | from json
assert not equal $remote_conflict.id $local_conflict.id

let output = tg --url $local.url push --group-children atomic | complete
failure $output "a conflict should reject the complete database-node batch"
assert ($output.stderr | str contains "the specifier is already in use")

failure (tg --url $remote.url group get atomic/a-new | complete)
let preserved = tg --url $remote.url group get atomic/conflict | from json
assert equal $preserved.id $remote_conflict.id
failure (tg --url $remote.url group get $local_new.id | complete)
failure (tg --url $remote.url group get $local_conflict.id | complete)
