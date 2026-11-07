use ../../util/test.nu *

# Cleaning accounts for checkout file bytes separately from cached object payloads.

let local = server spawn --name file

# Check in a file whose blob is stored through a checkout pointer.
let path = artifact 'hello'
let id = tg checkin --no-tokens --destructive --no-ignore --checkout-pointers $path | referent node
tg index
let object_bytes = tg object metadata $id | from json | get node.size

# Only the file object contributes cache bytes; the blob's contents contribute checkout bytes.
let output = tg clean | complete
success $output
let summary = $output.stdout | from json
assert equal $summary.checkouts 1
assert equal $summary.checkout_files 1
assert equal $summary.checkout_bytes 5
assert equal $summary.objects 2
assert equal $summary.object_bytes $object_bytes
failure (tg read $id | complete) 'the cleaned file should no longer be readable'

# A second clean reports no checkout files or bytes.
let output = tg clean | complete
success $output
let summary = $output.stdout | from json
assert equal $summary.checkout_files 0
assert equal $summary.checkout_bytes 0
assert equal $summary.objects 0
assert equal $summary.object_bytes 0
server stop $local

# Count files recursively when deleting a directory checkout.
let local = server spawn --name directory
let path = artifact {
	first: 'hello'
	link: (symlink 'first')
	nested: { second: 'world!' }
}
let id = tg checkin --no-tokens --destructive --no-ignore --checkout-pointers $path | referent node
let output = tg clean | complete
success $output
let summary = $output.stdout | from json
assert equal $summary.checkouts 1
assert equal $summary.checkout_files 2
assert equal $summary.checkout_bytes 11
failure (tg object get --bytes $id | complete) 'the cleaned directory should no longer be readable'
