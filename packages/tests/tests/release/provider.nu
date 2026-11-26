use ../../util/test.nu *
use ../../util/release.nu
use ../../../release/stage.nu

# A compiled Darwin target without its provider library does not replace a previously staged helper/provider pair.
let local = server spawn --name local
let path = release fixture
cd $path
'
	export const release = (arg: { commit: string; target: string }) =>
		tg.directory({ tangram: tg.file("new helper", { executable: true }) });
' | save -f build.tg.ts
^git -c user.name=test -c user.email=test@example.com -c commit.gpgsign=false commit -qam 'omit the provider'
let commit = ^git rev-parse HEAD | str trim
let target = 'aarch64-darwin'
mkdir release
'old helper' | save release/tangram_aarch64-darwin
'old provider' | save release/libtangram_vfs_provider_aarch64-darwin.a
stage record release $target $commit dir_0
let output = tg run .#release -- --stage compile --target $target | complete
failure $output
assert ($output.stderr | str contains 'missing libtangram_vfs_provider_aarch64-darwin.a')
assert equal (open --raw release/tangram_aarch64-darwin) 'old helper'
assert equal (open --raw release/libtangram_vfs_provider_aarch64-darwin.a) 'old provider'
