use ../lib/test.nu *

# Setup returns a reusable sync referent before transfer messages arrive and rejects missing, mismatched, and forged proofs.

const helper = path self ../lib/sync_control.py
let local = server spawn --name local
let socket = $local.url | str replace 'http+unix://' '' | url decode
let output = python3 $helper output $socket (which tg | first | get path) $local.url 0 '' $local.directory | complete
success $output 'the sync output should precede the transfer and preserve a supplied sync referent'
