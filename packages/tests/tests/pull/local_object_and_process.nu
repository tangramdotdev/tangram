use ../../util/test.nu *

# A pull of a stored object and a stored process together takes the local path and returns a token for each.

let local = server spawn

let object = tg put --no-tokens 'tg.file("stored object")' | referent node
let path = artifact { tangram.ts: 'export default () => "done";' }
let process = tg spawn $path | referent node
tg wait $process | ignore
tg index

# No remote is configured, so the pull succeeds only through the local path.
let output = tg --no-quiet pull --no-process-error-objects --no-process-output-objects $object $process | complete
success $output
assert not ($output.stderr | str contains 'tokens[') "the pull should not start a sync"
let referents = $output.stdout | lines
assert equal ($referents | length) 2
assert equal ($referents.0 | referent node) $object
assert equal ($referents.0 | referent tokens local | first | token body | get permissions) [object_subtree]
assert equal ($referents.1 | referent node) $process
assert equal ($referents.1 | referent tokens local | first | token body | get permissions) [process_node]
