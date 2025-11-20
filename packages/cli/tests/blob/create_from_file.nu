use ../../test.nu *

let server = spawn

let temp_file = mktemp -t
"hello, world!\n" | save -f $temp_file

# Write the file to create a blob.
let id = cat $temp_file | tg write | complete | get stdout | str trim

# Get the blob.
let output = tg get $id --blobs --depth=inf --pretty | complete
success $output

snapshot $output.stdout
