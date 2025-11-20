use ../../test.nu *

let server = spawn

let temp_file = mktemp -t
"hello!" | save -f $temp_file

# Check in the file.
let id = tg checkin $temp_file | complete | get stdout | str trim

# Bundle the file.
let bundle_id = tg bundle $id | complete | get stdout | str trim

# Get the bundled object.
let output = tg object get $bundle_id --blobs --depth=inf --pretty | complete
success $output

snapshot $output.stdout
