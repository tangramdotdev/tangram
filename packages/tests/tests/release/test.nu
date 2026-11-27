use ../../util/test.nu *
use ../../util/release.nu

# The test stage runs after the package stage and before the publish stage, so that a failed smoke test leaves the packages and publishes nothing.
if $nu.os-info.name != 'linux' or (^uname -m | str trim) != 'x86_64' {
	skip_test 'this test requires x86_64 linux'
}
let local = server spawn --name local
let state = release publisher
let path = release fixture
cd $path

# Make the smoke tests fail.
open --raw packages/release/build.tg.ts
	| str replace --regex 'export const test = [^\n]*\n[^\n]*\n' "export const test = () => { throw new Error(\"the smoke test failed\"); };\n"
	| save -f packages/release/build.tg.ts
^git -c user.name=test -c user.email=test@example.com -c commit.gpgsign=false commit -qam 'fail the smoke tests'

let output = tg run .#release -- --version 1.0.0 --target x86_64-linux-gnu | complete
failure $output
assert ($output.stderr | str contains 'the smoke test failed')
assert ('release/apt/tangram_1.0.0_amd64.deb' | path exists)
assert ('release/apt/Packages' | path exists)
let github = $state | path join github.json
assert (not ($github | path exists) or 'v1.0.0' not-in (open $github))
assert not ($state | path join bucket/stable/apt/Packages | path exists)
