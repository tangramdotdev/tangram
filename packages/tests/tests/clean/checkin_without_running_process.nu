use ../../util/test.nu *

# A clean with no process running removes a checked-in artifact that nothing references.

let local = server spawn

# Check in an artifact.
let path = artifact 'unreferenced'
let id = tg checkin --no-tokens $path | referent node
tg object get $id

# Clean, and check that the artifact is no longer readable.
let output = tg --no-quiet clean | complete
success $output 'the clean should succeed'
assert (not ($output.stderr | str contains 'because processes are running')) 'the clean should not be limited'
let get = tg object get $id | complete
failure $get 'the artifact should be removed'
