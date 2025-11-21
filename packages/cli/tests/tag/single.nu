use ../../test.nu *

let server = spawn

# Write the artifact to a temp.
let path = artifact 'test'

# Check in.
let id = run tg checkin $path

# Put tag.
let pattern = "test"
run tg tag put $pattern $id

# List tags.
let list_output = run tg tag list $pattern
snapshot -n list $list_output

# Get tag.
let get_output = run tg tag get $pattern
snapshot -n get $get_output
