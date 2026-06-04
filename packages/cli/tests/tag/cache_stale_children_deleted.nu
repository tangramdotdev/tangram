use ../../test.nu *

# A stale child tag is removed from the cache when a branch is refreshed after the child is deleted on the remote.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let path = artifact 'Hello, World!'
let id = tg --url $remote.url checkin $path
let old = tg --url $remote.url get $id | str trim

# Create a branch with two children and cache it.
tg --url $remote.url tag put "a/k/l" $id
tg --url $remote.url tag put "a/k/m" $id
let k = tg --url $local.url resolve "a/k" | str trim
assert equal $k $old "the branch should resolve to its newest child"

# Delete one child on the remote, then bust the cache.
tg --url $remote.url tag delete "a/k/l"
let k2 = tg --url $local.url resolve --ttl 0 "a/k" | str trim
assert equal $k2 $old "the branch should still resolve via the remaining child"

# The deleted child should be gone from the cache after the refresh.
let l = tg --url $local.url resolve --remote --cached --no-ttl "a/k/l" | complete
assert ($l.exit_code != 0) "the deleted child should be removed from the cache"
