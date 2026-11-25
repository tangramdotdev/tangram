use ../../util/test.nu *
use ../../util/release.nu
use ../../../../scripts/release/stage.nu

# The package stage fails before it packages anything when the machine cannot run containers for the architecture of a container image.
let local = server spawn --name local
let path = release fixture
cd $path
let commit = ^git rev-parse HEAD | str trim
let target = 'aarch64-linux-gnu'
mkdir release
'binary' | save release/tangram_aarch64-linux-gnu
^chmod 755 release/tangram_aarch64-linux-gnu
stage record release $target $commit

# Provide a Docker that cannot run containers for the architecture.
let bin = artifact {docker: (file --executable "#!/bin/sh\necho 'exec format error' >&2\nexit 255\n")}
$env.PATH = $env.PATH | prepend $bin
let output = tg run .#release -- --stage package --no-sign --target $target | complete
failure $output
assert ($output.stderr | str contains 'cannot run linux/arm64 containers')
assert ($output.stderr | str contains 'exec format error')
assert not ('release/tangram_aarch64-linux-gnu.tar.gz' | path exists)

# Building the container image with Tangram requires Linux.
if $nu.os-info.name != 'linux' {
	let output = tg run .#release -- --stage package --no-sign --tangram-image --target $target | complete
	failure $output
	assert ($output.stderr | str contains '--tangram-image requires Linux')
	assert not ('release/tangram_aarch64-linux-gnu.tar.gz' | path exists)
}
