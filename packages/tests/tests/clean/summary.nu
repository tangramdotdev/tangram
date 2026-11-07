use ../../util/test.nu *

# Cleaning reports the size of each removed object once and excludes retained objects.

let local = server spawn

# Store a file and its blob, repeat the blob put, and retain a different blob with a tag.
let blob = tg put --no-tokens 'tg.blob("hello")' | referent node
tg put --no-tokens 'tg.blob("hello")' | ignore
let file_value = ['tg.file({"contents":' $blob '})'] | str join
let file = tg put --no-tokens $file_value | referent node
let retained = tg put --no-tokens 'tg.blob("retained")' | referent node
tg tag retained $retained
tg index
let size = [$blob $file] | each { |id| tg object metadata $id | from json | get node.size } | math sum

# Check the reported count and byte total against the object's public metadata.
let output = tg --no-quiet clean | complete
success $output
let summary = $output.stdout | from json
assert equal ($summary | columns | sort) [cached_responses checkout_bytes checkout_files checkouts object_bytes objects processes sandboxes]
assert equal $summary.objects 2
assert equal $summary.object_bytes $size
assert equal $summary.checkout_files 0
assert equal $summary.checkout_bytes 0
assert (not ($output.stderr | str contains 'info cleaned')) $output.stderr
failure (tg object get --bytes $blob | complete) "the cleaned blob should no longer be readable"
failure (tg object get --bytes $file | complete) "the cleaned file should no longer be readable"
assert equal (tg read retained) 'retained'

# Cleaning again reports no additional objects or bytes.
let output = tg clean | complete
success $output
let summary = $output.stdout | from json
assert equal $summary.objects 0
assert equal $summary.object_bytes 0
