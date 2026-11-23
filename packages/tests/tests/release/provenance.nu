use ../../util/test.nu *
use ../../util/release.nu

# Release transfers reject stale source revisions and changed binaries, and preserve valid compiled artifacts.
let remote = server spawn --name remote
let local = server spawn --name local --config {remotes: {default: {url: $remote.url}}}
let path = release fixture
cd $path
let commit = ^git rev-parse HEAD | str trim
let host = ^rustc -vV | lines | where {$in starts-with 'host: '} | first | str replace 'host: ' ''
let target = $host | str replace '-apple-' '-' | str replace '-unknown-' '-'
let binary = $path | path join release $'tangram_($target)'

# Compile into the explicitly selected target directory.
let output = tg run .#release -- --stage compile --cargo --target $target --target-dir 'target/reused directory' | complete
success $output
assert equal (^$binary | str trim) $commit

# Reject an old binary after the source changes.
let original = open --raw cli/main.rs
$original + "\n// source edit\n" | save -f cli/main.rs
let output = tg run .#release -- --stage push --force --target $target | complete
failure $output
assert ($output.stderr | str contains 'does not match the current source revision')
let output = tg run .#release -- --stage package --no-sign --target $target | complete
failure $output
assert ($output.stderr | str contains 'does not match the current source revision')
$original | save -f cli/main.rs

# Reject a modified staged binary before pushing or packaging it.
let original = open --raw $binary
'modified binary' | save -f $binary
let output = tg run .#release -- --stage push --force --target $target | complete
failure $output
assert ($output.stderr | str contains 'checksum does not match')
let output = tg run .#release -- --stage package --no-sign --target $target | complete
failure $output
assert ($output.stderr | str contains 'checksum does not match')
$original | save -f $binary
^chmod 755 $binary

# Push and pull a valid release through a separate server.
let output = tg run .#release -- --stage push --target $target | complete
success $output
let consumer = server spawn --name consumer --config {remotes: {default: {url: $remote.url}}}
rm $binary
let output = tg run .#release -- --stage pull --target $target | complete
success $output
assert equal (^$binary | str trim) $commit
