use ../lib/test.nu *

# A directory cannot be checksummed.

let local = server spawn

let dir = tg put --no-tokens 'tg.directory({ "link": tg.symlink({ "artifact": tg.file("target") }) })' | referent node

let output = tg checksum $dir | complete
failure $output
assert ($output.stderr | str contains "expected a blob or file")
