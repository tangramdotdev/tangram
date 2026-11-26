use ../../util/test.nu *
use ../../util/release.nu
use ../../../../scripts/release/stage.nu

# The package stage fails before it packages anything when it would build a container image on a machine that is not Linux.
if $nu.os-info.name == 'linux' {
	skip_test 'this test requires a machine that is not linux'
}
let local = server spawn --name local
let path = release fixture
cd $path
let commit = ^git rev-parse HEAD | str trim
let target = 'x86_64-linux-gnu'
mkdir release
'binary' | save release/tangram_x86_64-linux-gnu
^chmod 755 release/tangram_x86_64-linux-gnu
stage record release $target $commit
let output = tg run .#release -- --stage package --no-sign --target $target | complete
failure $output
assert ($output.stderr | str contains 'building the container image for x86_64-linux-gnu requires Linux')
assert not ('release/tangram_x86_64-linux-gnu.tar.gz' | path exists)
