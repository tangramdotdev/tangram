use ../../test.nu *

# A symlink cannot be checksummed.

let server = server spawn

let symlink_id = tg put 'tg.symlink({ "path": "some/path" })' | str trim

let output = tg checksum $symlink_id | complete
failure $output
assert ($output.stderr | str contains "expected a blob or file")
