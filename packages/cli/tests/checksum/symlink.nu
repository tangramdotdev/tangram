use ../../test.nu *

# A symlink artifact with a path target can be checksummed.

let server = spawn

let symlink_id = tg put 'tg.symlink({ "path": "some/path" })' | str trim

let checksum = tg checksum $symlink_id | from json
assert ($checksum | str starts-with "sha256:") "the symlink checksum should be a sha256 checksum"
