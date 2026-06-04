use ../../test.nu *

# Bundling a checked-in file with no dependencies produces a bundled object that matches the snapshot.

let server = spawn

let temp_file = mktemp --tmpdir
"hello!" | save --force $temp_file

# Check in the file.
let id = tg checkin $temp_file

# Bundle the file.
let bundle_id = tg bundle $id

# Get the bundled object.
let output = tg object get $bundle_id --blobs --depth=inf --pretty
snapshot $output
