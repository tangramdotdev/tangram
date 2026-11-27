use ../../util/test.nu *
use ../../util/release.nu

# A release that would publish a target that this machine cannot test, or publish without the test stage, refuses before any work starts, and publishes with --untested.
if $nu.os-info.name != 'linux' or (^uname -m | str trim) != 'x86_64' {
	skip_test 'this test requires x86_64 linux'
}
let local = server spawn --name local
let state = release publisher
let path = release fixture
cd $path

let output = tg run .#release -- --version 1.0.0 --target aarch64-linux-gnu | complete
failure $output
assert ($output.stderr | str contains 'this machine cannot test aarch64-linux-gnu')
assert ($output.stderr | str contains '--untested')
assert not ('release' | path exists)

let output = tg run .#release -- --stage publish --version 1.0.0 --target x86_64-linux-gnu | complete
failure $output
assert ($output.stderr | str contains 'publishing requires the test stage')
assert not ('release' | path exists)

let output = tg run .#release -- --version 1.0.0 --target aarch64-linux-gnu --untested | complete
success $output
let github = open ($state | path join github.json) | get 'v1.0.0'
assert equal ($github.assets | columns) ['tangram_aarch64-linux-gnu.tar.gz']
