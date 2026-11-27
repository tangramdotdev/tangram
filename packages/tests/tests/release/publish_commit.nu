use ../../util/test.nu *
use ../../util/release.nu

# A run from another commit refuses to add to a published release before it compiles anything, and leaves the release as it was.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
let local = server spawn --name local
let state = release publisher
let path = release fixture
cd $path
let commit = ^git rev-parse HEAD | str trim
let output = tg run .#release -- --version 1.0.0 --target x86_64-linux-gnu | complete
success $output
let published = release published $state

# Commit a change, and add another target from the new commit.
"\nedit\n" | save --append source.txt
^git -c user.name=test -c user.email=test@example.com -c commit.gpgsign=false commit -qam edit
let output = tg run .#release -- --version 1.0.0 --target aarch64-linux-gnu --untested | complete
failure $output
assert ($output.stderr | str contains $'release 1.0.0 was published from commit ($commit)')
assert not ('release/tangram_aarch64-linux-gnu' | path exists)
assert equal (release published $state) $published
