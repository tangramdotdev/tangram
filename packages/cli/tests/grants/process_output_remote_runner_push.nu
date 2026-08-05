use ../../test.nu *

# Verify whether a remote runner can spawn a process created by a credentialed user, push its output, and the user can see the result.

def tangram_encode [] {
	let standard = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567" | split chars
	let tangram = "0123456789abcdefghjkmnpqrstvwxyz" | split chars
	let map = $standard | zip $tangram | reduce -f {} {|pair, acc| $acc | insert ($pair | get 0) ($pair | get 1) }
	$in | encode base32 --nopad | split chars | each {|char| $map | get $char } | str join
}

# Create a fake runner + token.
let runner_id = "rnr_0000000000000000000000000000"
let runner_token = do {
	let private_key = 'U9ZBC697GDA0dlUBF/VVM4eqoJUVfQqwRNr6L2z8Ajg=' | decode base64
	let pkcs8_prefix = 0x[302e020100300506032b657004220420]
	let key_path = mktemp -t
	bytes build $pkcs8_prefix $private_key | save --force $key_path
	let body = '{"expires_at":9223372036854775807,"issued_at":0,"principal":{"kind":"runner","value":"' + $runner_id + '"}}'
	let metadata = '{"algorithm":"ed25519","key":"default"}'
	let input = 'authentication.0.' + ($body | tangram_encode) + '.' + ($metadata | tangram_encode)
	let input_path = mktemp -t
	$input | save --force --raw $input_path
	let signature_path = mktemp -t
	^openssl pkeyutl -sign -rawin -inkey $key_path -keyform DER -in $input_path -out $signature_path
	$input + '.' + (open --raw $signature_path | into binary | tangram_encode)
}

# Spawn the remote.
let remote = spawn --cloud --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { users: { providers: { insecure: true } } },
	roles: [cleaner finalizer http indexer scheduler],
}

# Spawn the runner.
let runner = spawn --name runner --config {
	remotes: { default: { token: $runner_token, url: $remote.url } },
	runner: { id: $runner_id, remote: "default", token: $runner_token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose alice | from json
let local = spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Run a build that returns an object.
let path = artifact {
	tangram.ts: 'export default function () { return tg.file("hello"); }'
}
let result = tg --url $local.url build --remote $path | complete
success $result

# Verify the user can read the output.
let file = $result.stdout | str trim
let output = tg --url $local.url get $file | complete
success $output
snapshot $output.stdout '
	tg.file({"contents":blb_01t10ptmtyxpb108ztd4np15vt0jm9qnfkfny07vr8yp7tebj04dgg})

'
