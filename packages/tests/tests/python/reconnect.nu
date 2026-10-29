use ../../util/test.nu *

# A detached process preserves its stdio cursors across an HTTP/2 disconnect.

const script_path = path self 'reconnect.py'

let local = server spawn
let output = ^python $script_path | complete
success $output
assert equal ($output.stdout | str trim) 'process reconnected without losing or repeating stdio' 'the Python client should reconnect its process transport'
