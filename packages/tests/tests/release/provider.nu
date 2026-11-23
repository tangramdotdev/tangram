use ../../util/test.nu *
use ../../util/release.nu
use ../../../../scripts/release/stage.nu

# An incomplete Darwin release cannot replace a previously staged helper/provider pair.
let remote = server spawn --name remote
let path = release fixture
cd $path
let commit = ^git rev-parse HEAD | str trim
let target = 'aarch64-darwin'
let incoming = artifact {
	'tangram_aarch64-darwin': 'new helper'
	'libtangram_vfs_provider_aarch64-darwin.a': 'new provider'
}
stage record $incoming $target $commit
rm ($incoming | path join libtangram_vfs_provider_aarch64-darwin.a)
let id = tg checkin --no-tokens $incoming | referent node
tg tag put --create-ancestors $'tangram/release/($commit)/($target)' $id

let local = server spawn --name local --config {remotes: {default: {url: $remote.url}}}
mkdir release
'old helper' | save release/tangram_aarch64-darwin
'old provider' | save release/libtangram_vfs_provider_aarch64-darwin.a
stage record release $target $commit
let output = tg run .#release -- --stage pull --target $target | complete
failure $output
assert ($output.stderr | str contains 'missing libtangram_vfs_provider_aarch64-darwin.a')
assert equal (open --raw release/tangram_aarch64-darwin) 'old helper'
assert equal (open --raw release/libtangram_vfs_provider_aarch64-darwin.a) 'old provider'
