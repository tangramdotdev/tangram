use ../../test.nu *

# Pull supports never, missing, and always ancestor handling.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}
let empty = spawn --name empty --config {
	remotes: { default: { url: $remote.url } }
}
let coalesced = spawn --name coalesced --config {
	remotes: { default: { url: $remote.url } }
}
let recursive = spawn --name recursive --config {
	remotes: { default: { url: $remote.url } }
}
let scoped = spawn --name scoped --config {
	remotes: { default: { url: $remote.url } }
}

let remote_parent = tg --url $remote.url group create parent | from json
let remote_child = tg --url $remote.url group create parent/child | from json
let remote_grandchild = tg --url $remote.url group create parent/child/grandchild | from json
let remote_other = tg --url $remote.url group create parent/child/other | from json
tg --url $remote.url group create parent/sibling | ignore

# Never rejects a missing parent.
failure (tg --url $empty.url pull --ancestors=never parent/child | complete)

# Missing rejects a conflicting parent.
let local_parent = tg --url $local.url group create parent | from json
let local_child = tg --url $local.url group create parent/child | from json
let local_descendant = tg --url $local.url group create parent/child/local | from json
assert not equal $local_parent.id $remote_parent.id
failure (tg --url $local.url pull --ancestors=missing parent/child | complete)
assert equal (tg --url $local.url group get parent | from json | get id) $local_parent.id

# Always replaces overlapping conflicting roots and deletes their old subtree.
tg --url $local.url pull --ancestors=always parent/child
assert equal (tg --url $local.url group get parent | from json | get id) $remote_parent.id
assert equal (tg --url $local.url group get parent/child | from json | get id) $remote_child.id
failure (tg --url $local.url group get parent/child/local | complete)
failure (tg --url $local.url group get $local_child.id | complete)
failure (tg --url $local.url group get $local_descendant.id | complete)
failure (tg --url $local.url group get $local_parent.id | complete)

# A recursive request upgrades an earlier non-recursive ancestor request without resending the node.
tg --url $coalesced.url pull --group-children parent/child/grandchild parent
assert equal (
	tg --url $coalesced.url group get parent/child/other | from json | get id
) $remote_other.id

# Pulling children replaces a conflicting descendant when the requested root already matches.
tg --url $recursive.url pull parent
let local_child = tg --url $recursive.url group create parent/child | from json
assert not equal $local_child.id $remote_child.id
tg --url $recursive.url pull --group-children parent
assert equal (tg --url $recursive.url group get parent/child | from json | get id) $remote_child.id

# Pulling a subtree does not pull siblings through a dynamically requested ancestor.
tg --url $scoped.url pull --group-children parent/child
failure (tg --url $scoped.url group get --local parent/sibling | complete)
