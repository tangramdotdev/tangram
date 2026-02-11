use ../../test.nu *

# Spawn a remote server and a local server.
let remote = spawn --cloud -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
	tag: { cache_ttl: 100 }
}

# Create an artifact on the remote.
let path = artifact 'Hello, World!'
let id = tg -u $remote.url checkin $path

# Put tags on the remote to form a tree:
#   a/b
#   a/c/d
#   a/c/e/f/g
tg -u $remote.url tag put "a/b" $id
tg -u $remote.url tag put "a/c/d" $id
tg -u $remote.url tag put "a/c/e/f/g" $id

# Prime the cache by fetching a/b through the local server.
tg -u $local.url tag get "a/b"

# Update a/b on the remote.
let path2 = artifact 'Goodbye, World!'
let id2 = tg -u $remote.url checkin $path2
tg -u $remote.url tag put -f "a/b" $id2

# Within TTL, should still return the old cached item.
let cached = tg -u $local.url tag get "a/b" | from json
assert ($cached.item == $id) "within TTL should return old item"

# With --ttl 0, should return the new item.
let fresh = tg -u $local.url tag get --ttl 0 "a/b" | from json
assert ($fresh.item == $id2) "ttl 0 should return new item"

# Fetch c.
let c1 = tg -u $local.url tag get "a/c"
snapshot $c1 '{"children":[{"component":"d","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"component":"e"}],"remote":"default","tag":"a/c"}'

# Add another child to c. Within ttl, should be the same as the last one.
tg -u $remote.url tag put -f "a/c/h" $id2
let c2 = tg -u $local.url tag get "a/c"
assert ($c2 == $c1) "within TTL should return the same result"

# With --ttl 0, should return the new child.
let c3 = tg -u $local.url tag get --ttl 0 "a/c"
snapshot $c3 '{"children":[{"component":"d","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"component":"e"},{"component":"h","item":"fil_01ne07s5fgpn2cxfg2b5xgasfrfcjev22snh7g2nfjxpy02ady2yfg"}],"remote":"default","tag":"a/c"}'

# Test that intermediate nodes are not cached.
tg -u $local.url tag get "a/c/e/f/g"

# Add a sibling branch on the remote.
tg -u $remote.url tag put "a/c/e/i/j" $id

# Fetch e, should return the new sibling i.
let e = tg -u $local.url tag get "a/c/e"
snapshot $e '{"children":[{"component":"f"},{"component":"i"}],"remote":"default","tag":"a/c/e"}'

# Stale children are deleted when the cache is refreshed.
# Create a branch with two children on the remote.
tg -u $remote.url tag put "a/k/l" $id
tg -u $remote.url tag put "a/k/m" $id

# Fetch the branch to cache both children.
let k = tg -u $local.url tag get "a/k"
snapshot $k '{"children":[{"component":"l","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"component":"m","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"}],"remote":"default","tag":"a/k"}'

# Delete one child on the remote.
tg -u $remote.url tag delete "a/k/l"

# Bust the cache and fetch again. The deleted child should be gone.
let k2 = tg -u $local.url tag get --ttl 0 "a/k"
snapshot $k2 '{"children":[{"component":"m","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"}],"remote":"default","tag":"a/k"}'

# A branch that becomes a leaf should have its cached children cleaned up.
# Create a branch with children on the remote.
tg -u $remote.url tag put "a/n/o" $id
tg -u $remote.url tag put "a/n/p" $id

# Fetch the branch to cache the children.
let n = tg -u $local.url tag get "a/n"
snapshot $n '{"children":[{"component":"o","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"component":"p","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"}],"remote":"default","tag":"a/n"}'

# Replace the branch with a leaf on the remote.
tg -u $remote.url tag delete "a/n/o"
tg -u $remote.url tag delete "a/n/p"
tg -u $remote.url tag delete "a/n"
tg -u $remote.url tag put "a/n" $id

# Bust the cache. Should return a leaf, not a branch with children.
let n2 = tg -u $local.url tag get --ttl 0 "a/n"
snapshot $n2 '{"item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60","remote":"default","tag":"a/n"}'

# A child cached as part of a branch fetch can be updated individually.
# Fetch a to cache its children including b.
tg -u $local.url tag get --ttl 0 "a"

# Update b on the remote.
let path3 = artifact 'Final version'
let id3 = tg -u $remote.url checkin $path3
tg -u $remote.url tag put -f "a/b" $id3

# Bust the cache for b specifically. Should return the new item.
let b = tg -u $local.url tag get --ttl 0 "a/b" | from json
assert ($b.item == $id3) "child updated individually should return new item"
