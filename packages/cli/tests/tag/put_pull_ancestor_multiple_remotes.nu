use ../lib/test.nu *

# Ancestor pulling continues until a remote supplies the immediate parent.

let local_first = server spawn --cloud --name local-first
let local_second = server spawn --cloud --name local-second

let root = tg --url $local_first.url group create foo | from json
tg --url $local_first.url remote put second $local_second.url
tg --url $local_first.url push --remote=second foo
let remote_parent = tg --url $local_second.url group create foo/bar | from json

let local = server spawn --name local --config {
	remotes: {
		first: { url: $local_first.url }
		second: { url: $local_second.url }
	}
}
let node = tg --url $local.url put 'tg.file("data")' | str trim
tg --url $local.url tag put -p foo/bar/baz $node

let local_root = tg --url $local.url group get foo | from json
let local_parent = tg --url $local.url group get foo/bar | from json
assert equal $local_root.id $root.id
assert equal $local_parent.id $remote_parent.id

# Always continues past a remote that is missing an existing local parent, then rejects conflicts.
let remote_refresh = tg --url $local_second.url group create refresh | from json
let remote_refresh_parent = tg --url $local_second.url group create refresh/parent | from json
let local_refresh = tg --url $local.url group create refresh | from json
let local_refresh_parent = tg --url $local.url group create refresh/parent | from json
assert not equal $local_refresh.id $remote_refresh.id
assert not equal $local_refresh_parent.id $remote_refresh_parent.id

let output = tg --url $local.url tag put -p --pull-ancestors=always refresh/parent/tag $node | complete
failure $output "always should reject a conflicting ancestor from a later remote"
assert ($output.stderr | str contains "the specifier is already in use")

let actual_refresh = tg --url $local.url group get refresh | from json
let actual_refresh_parent = tg --url $local.url group get refresh/parent | from json
assert equal $actual_refresh.id $local_refresh.id
assert equal $actual_refresh_parent.id $local_refresh_parent.id
failure (tg --url $local.url tag get refresh/parent/tag | complete)
