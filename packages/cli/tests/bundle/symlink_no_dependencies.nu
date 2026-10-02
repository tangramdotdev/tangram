use ../lib/test.nu *

# Bundling a symlink with no dependencies returns the symlink unchanged.

let local = server spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.symlink("target"); }'
}
let id = tg build --no-tokens $path | str trim

let bundle_id = tg bundle --no-tokens $id | referent node
assert equal $bundle_id $id "bundling a dependency-free symlink should return it unchanged"
