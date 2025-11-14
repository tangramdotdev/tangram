use ../../test.nu *

let server = spawn

# Write the artifact to a temp.
let path = artifact 'test'

# Check in.
let id = tg checkin $path

# Put tag.
let pattern = "test"
tg tag put $pattern $id

# List tags.
let list_output = tg tag list $pattern
snapshot -n list $list_output

# Get tag.
let get_output = tg tag get $pattern
snapshot -n get $get_output
