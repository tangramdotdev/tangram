use ../../test.nu *

# Writing a file through standard input creates a blob whose retrieved contents match the snapshot.

let server = spawn

let temp_file = mktemp --tmpdir
"hello, world!\n" | save --force $temp_file

# Write the file to create a blob.
let id = cat $temp_file | tg write

# Get the blob.
let output = tg get $id --blobs --depth=inf --pretty
snapshot $output
