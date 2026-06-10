use ../../test.nu *

# Cleaning deletes cached remote tags and the tags can be re-fetched from the remote afterward.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let path = artifact 'Hello, World!'
let id = tg --url $remote.url checkin $path
let old = tg --url $remote.url get $id | str trim
tg --url $remote.url tag put "a/b" $id
tg --url $remote.url tag put "a/c/d" $id

# Cache a remote tag tree by resolving through the local server.
tg --url $local.url resolve "a/b" | ignore
tg --url $local.url resolve "a/c/d" | ignore

# The tags are cached. Use --no-ttl to bypass the expiry check.
let before_b = tg --url $local.url resolve --remote --cached --no-ttl "a/b" | complete
assert ($before_b.exit_code == 0) "tag a/b should be in the cache before clean"
let before_d = tg --url $local.url resolve --remote --cached --no-ttl "a/c/d" | complete
assert ($before_d.exit_code == 0) "tag a/c/d should be in the cache before clean"

# Clean deletes all cached remote tags.
tg --url $local.url clean
let after_b = tg --url $local.url resolve --remote --cached --no-ttl "a/b" | complete
assert ($after_b.exit_code != 0) "leaf tag a/b should be cleaned"
let after_d = tg --url $local.url resolve --remote --cached --no-ttl "a/c/d" | complete
assert ($after_d.exit_code != 0) "leaf tag a/c/d should be cleaned"

# The tags are still available from the remote.
let refetched = tg --url $local.url resolve "a/b" | str trim
assert equal $refetched $old "the tag should be re-fetched from the remote after clean"
