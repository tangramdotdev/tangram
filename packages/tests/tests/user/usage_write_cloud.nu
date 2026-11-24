use ../../util/test.nu *

# Writing a blob with several chunks to a cloud-backed server charges the writer for every object of the blob.

let local = server spawn --cloud --config {
	authentication: { users: { providers: { insecure: true } } },
	usage: true,
}
tg login --name alice

# Alice writes a blob that is split into several chunks.
let blob = seq 1 100000 | str join "\n" | tg write --no-tokens | referent node
tg index
let metadata = tg object metadata $blob | from json
assert ($metadata.subtree.count > 1) "the blob should have children"

# Alice is charged for every object of the blob.
let usage = tg user usage | from json
assert equal $usage.object_count $metadata.subtree.count
assert equal $usage.object_size $metadata.subtree.size
