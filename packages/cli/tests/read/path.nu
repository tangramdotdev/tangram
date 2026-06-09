use ../../test.nu *

# Reading a filesystem path checks it in and returns the file contents.

let server = spawn

let temp_file = mktemp --tmpdir
"path contents" | save --force $temp_file

let contents = tg read $temp_file
assert equal $contents "path contents" "the read contents should match the file on disk"
