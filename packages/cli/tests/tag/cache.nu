use ../../test.nu *

# Spawn a remote server and a local server.
let remote = spawn --cloud -n remote
let local = spawn -n local -c {
	remotes: { default: { url: $remote.url } }
}

# Create an artifact on the remote.
let path = artifact 'Hello, World!'
let id = tg -u $remote.url checkin $path
let output = 'tg.file({"contents":blb_01b7mbpwtwk7vv4n50rn5cab07zcxvpq8d7pggwc2g54d0cjd8nnm0})'

# Put tags on the remote to form a tree:
#   a/b
#   a/c/d
#   a/c/e/f/g
tg -u $remote.url tag put "a/b" $id
tg -u $remote.url tag put "a/c/d" $id
tg -u $remote.url tag put "a/c/e/f/g" $id

# Prime the cache by fetching a/b through the local server.
tg -u $local.url resolve "a/b" | ignore

# Update a/b on the remote.
let path2 = artifact 'Goodbye, World!'
let id2 = tg -u $remote.url checkin $path2
let output2 = 'tg.file({"contents":blb_014ynsq1gxtm90pp08hw2f5nbbrnnss8n4vj6tydmt13kr7j1q7tgg})'
tg -u $remote.url tag put -f "a/b" $id2

# Within TTL, should still return the old cached item.
let cached = tg -u $local.url resolve "a/b" | str trim
assert equal $cached $output

# With --ttl 0, should return the new item.
let fresh = tg -u $local.url resolve --ttl 0 "a/b" | str trim
assert equal $fresh $output2

# Fetch c.
let c1 = tg -u $local.url resolve "a/c" | str trim
assert equal $c1 $output

# Add another child to c. Within ttl, should be the same as the last one.
tg -u $remote.url tag put -f "a/c/h" $id2
let c2 = tg -u $local.url resolve "a/c" | str trim
assert ($c2 == $c1) "within TTL should return the same result"

# With --ttl 0, should return the new child.
let c3 = tg -u $local.url resolve --ttl 0 "a/c" | str trim
assert equal $c3 $output2

# Test that descendant tags can be cached independently.
tg -u $local.url resolve "a/c/e/f/g" | ignore

# Add a sibling branch on the remote.
tg -u $remote.url tag put "a/c/e/i/j" $id

# Fetch the new descendant.
let e = tg -u $local.url resolve "a/c/e/i/j" | str trim
assert equal $e $output

# Stale children are deleted when the cache is refreshed.
# Create a branch with two children on the remote.
tg -u $remote.url tag put "a/k/l" $id
tg -u $remote.url tag put "a/k/m" $id

# Fetch the branch to cache both children.
let k = tg -u $local.url resolve "a/k" | str trim
assert equal $k $output

# Delete one child on the remote.
tg -u $remote.url tag delete "a/k/l"

# Bust the cache and fetch again. The deleted child should be gone.
let k2 = tg -u $local.url resolve --ttl 0 "a/k" | str trim
assert equal $k2 $output

# A child cached as part of a branch fetch can be updated individually.
# Fetch a to cache its children including b.
tg -u $local.url resolve --ttl 0 "a" | ignore

# Update b on the remote.
let path3 = artifact 'Final version'
let id3 = tg -u $remote.url checkin $path3
let output3 = 'tg.file({"contents":blb_01k44gpq46bpjmfap4k6gvqaccen1c92y9fmtyb2cc1mzpvmnvb6vg})'
tg -u $remote.url tag put -f "a/b" $id3

# Bust the cache for b specifically. Should return the new item.
let b = tg -u $local.url resolve --ttl 0 "a/b" | str trim
assert equal $b $output3

# A child tag that is cached should not be removed when we later get its parent.
# Create a branch with children on the remote.
tg -u $remote.url tag put "a/q/r/s" $id
tg -u $remote.url tag put "a/q/t" $id
let s1 = tg -u $local.url resolve "a/q/r/s"
let q = tg -u $local.url resolve "a/q" | ignore

# Fetch s again. It should still be in the cache.
let s2 = tg -u $local.url resolve --remote --cached "a/q/r/s"
assert ($s1 == $s2)

# Test cleaning of cached remote tags.
let clean_local = spawn -n clean_local -c {
	remotes: { default: { url: $remote.url } }
}

# Cache a remote tag tree by fetching from the remote.
tg -u $clean_local.url resolve "a/b" | ignore
tg -u $clean_local.url resolve "a/c/d" | ignore

# Verify the tags are cached. Use a large ttl to bypass the expiry check.
let before_b = tg -u $clean_local.url resolve --remote --cached --no-ttl "a/b" | complete
assert ($before_b.exit_code == 0) "tag a/b should be in the cache before clean"
let before_d = tg -u $clean_local.url resolve --remote --cached --no-ttl "a/c/d" | complete
assert ($before_d.exit_code == 0) "tag a/c/d should be in the cache before clean"

# Clean. All cached remote tags are deleted.
tg -u $clean_local.url clean

# The cached remote leaf tags should be deleted from the database.
let after_b = tg -u $clean_local.url resolve --remote --cached --no-ttl "a/b" | complete
assert ($after_b.exit_code != 0) "leaf tag a/b should be cleaned"
let after_d = tg -u $clean_local.url resolve --remote --cached --no-ttl "a/c/d" | complete
assert ($after_d.exit_code != 0) "leaf tag a/c/d should be cleaned"

# Tags are still available from the remote. Fetching should work after clean.
let refetched = tg -u $clean_local.url resolve "a/b" | str trim
assert equal $refetched $output3
