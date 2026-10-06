use ../lib/test.nu *

# A detached process preserves its stdio cursors across an HTTP/2 disconnect.

const python_path = path self '../../../../.venv/bin/python'
const script_path = path self 'reconnect.py'

let local = server spawn
let output = ^$python_path $script_path | complete
success $output
assert equal ($output.stdout | str trim) 'process reconnected without losing or repeating stdio' 'the Python client should reconnect its process transport'
