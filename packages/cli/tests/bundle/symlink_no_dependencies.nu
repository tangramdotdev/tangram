use ../../test.nu *

# Bundling a symlink with no dependencies returns the symlink unchanged.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.symlink("target"); }'
}
let id = tg build $path | str trim

let bundle_id = tg bundle $id | str trim
assert equal $bundle_id $id "bundling a dependency-free symlink should return it unchanged"
