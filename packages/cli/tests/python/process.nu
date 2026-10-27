use ../lib/test.nu *

# A Python process connection can write stdin while reading stdout over HTTP/2.

const script_path = path self 'process.py'

let local = server spawn
let output = ^python $script_path | complete
success $output
assert equal ($output.stdout | str trim) 'duplex process completed' 'the Python client should stream process stdio'
