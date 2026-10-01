use ../lib/test.nu *

# Setup returns a reusable sync referent before transfer messages arrive and rejects missing, mismatched, and forged proofs.

const helper = path self ../lib/sync_control.py
let root_token = random chars
let local = server spawn --name local --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}
let socket = $local.url | str replace 'http+unix://' '' | url decode
let output = python3 $helper output $socket (which tg | first | get path) $local.url 0 '' $local.directory $root_token | complete
success $output 'the sync output should precede the transfer and preserve a supplied sync referent'
