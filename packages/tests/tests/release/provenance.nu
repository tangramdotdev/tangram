use ../../util/test.nu *
use ../../util/release.nu

# Packaging rejects a staged binary from an older source revision and a changed staged binary.
let local = server spawn --name local
let path = release fixture
cd $path
let target = release host-target
let binary = $path | path join release $'tangram_($target)'
let output = tg run .#release -- --stage compile --target $target | complete
success $output

# Reject an old binary after the source changes.
let original = open --raw source.txt
$original + "\nedit\n" | save -f source.txt
let output = tg run .#release -- --stage package --no-sign --version 0.0.0 --target $target | complete
failure $output
assert ($output.stderr | str contains 'does not match the current source revision')
$original | save -f source.txt

# Reject a modified staged binary.
'modified binary' | save -f $binary
let output = tg run .#release -- --stage package --no-sign --version 0.0.0 --target $target | complete
failure $output
assert ($output.stderr | str contains 'checksum does not match')
assert not ($'($binary).tar.gz' | path exists)
