use ../../util/test.nu *
use ../../util/release.nu

# Publishing the same targets again from the same commit replaces them, and leaves the release as the first run published it.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
let local = server spawn --name local
let state = release publisher
let path = release fixture
cd $path
let output = tg run .#release -- --version 1.0.0 --target x86_64-linux-gnu --target x86_64-linux-musl | complete
success $output
let published = release published $state
assert equal ($published.aur | length) 1
let apk = open --raw ($state | path join bucket/stable/apk/x86_64/APKINDEX.tar.gz)
assert ($apk starts-with 'tangramdotdev-1.0.0-r0.apk: x86_64-linux-musl 1.0.0 from')

let output = tg run .#release -- --version 1.0.0 --target x86_64-linux-gnu --target x86_64-linux-musl | complete
success $output
assert equal (release published $state) $published
