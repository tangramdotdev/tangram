use ../../test.nu *

let server = spawn

let temp_file = mktemp -t
"hello, world!\n" | save -f $temp_file

# Write the file to create a blob.
let id = cat $temp_file | tg write

# Get the blob.
let output = run tg get $id --blobs --depth=inf --pretty
snapshot $output
