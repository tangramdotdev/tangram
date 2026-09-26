use ../lib/test.nu *

const helper = path self ../lib/sync_control.py

# Setup returns a reusable sync referent before transfer messages arrive and rejects missing, mismatched, and forged proofs.
let server = server spawn
let socket = $server.url | str replace 'http+unix://' '' | url decode
let output = python3 $helper output $socket (which tg | first | get path) $server.url 0 '' $server.directory | complete
success $output 'the sync output should precede the transfer and preserve a supplied sync referent'
