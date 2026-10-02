use ../lib/test.nu *

# A symlink cannot be checksummed.

let local = server spawn

let symlink_id = tg put --no-tokens 'tg.symlink({ "path": "some/path" })' | referent node

let output = tg checksum $symlink_id | complete
failure $output
assert ($output.stderr | str contains "expected a blob or file")
