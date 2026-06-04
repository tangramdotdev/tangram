use ../../test.nu *

# A cached child tag is not evicted when its parent branch is later fetched.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let path = artifact 'Hello, World!'
let id = tg --url $remote.url checkin $path
tg --url $remote.url tag put "a/q/r/s" $id
tg --url $remote.url tag put "a/q/t" $id

# Cache the child, then fetch its parent.
let s1 = tg --url $local.url resolve "a/q/r/s" | str trim
tg --url $local.url resolve "a/q" | ignore

# The child should still be in the cache.
let s2 = tg --url $local.url resolve --remote --cached "a/q/r/s" | str trim
assert equal $s1 $s2 "the child tag should still be cached after fetching its parent"
