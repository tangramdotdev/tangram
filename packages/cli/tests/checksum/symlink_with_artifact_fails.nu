use ../../test.nu *

# A directory cannot be checksummed.

let server = server spawn

let dir = tg put 'tg.directory({ "link": tg.symlink({ "artifact": tg.file("target") }) })' | str trim

let output = tg checksum $dir | complete
failure $output
assert ($output.stderr | str contains "expected a blob or file")
