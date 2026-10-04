use ../lib/test.nu *

# Filesystem modules require root, while embedded declarations remain available to all callers.

let directory = mktemp -d
let secret_path = $directory | path join secret.tg.ts
let secret = 'export default "server-side secret";'
$secret | save $secret_path

let root_token = random chars
let port = random int 20000..50000
let url = $'http://127.0.0.1:($port)'
let local = server spawn --url $url --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}
let alice = tg --url $local.url login --verbose --name alice | from json
let object = tg --url $local.url --token $alice.token put --no-tokens 'tg.file("export default 42;")' | referent node

for token in [null $alice.token $root_token] {
	let headers = if $token == null {
		{ 'Content-Type': application/json }
	} else {
		{ 'Content-Type': application/json, Authorization: $'Bearer ($token)' }
	}
	for kind in [js ts] {
		let body = { module: { kind: $kind, referent: { node: $secret_path, options: {} } } }
		let response = ($body | to json --raw)
			| http post --allow-errors --full --headers $headers $'($local.url)/modules/load'
		if $token == $root_token {
			assert equal $response.status 200
			assert equal $response.body.text $secret
		} else {
			assert ($response.status != 200) "only root may read a filesystem module"
			assert ($response.body | to json --raw | str contains "unauthorized")
		}
	}

	# Declarations come from the embedded library, even for unauthenticated callers.
	let body = { module: { kind: dts, referent: { node: './lib.es5.d.ts', options: {} } } }
	let response = ($body | to json --raw)
		| http post --allow-errors --full --headers $headers $'($local.url)/modules/load'
	assert equal $response.status 200
	assert ($response.body.text | str contains 'interface Object')

	# Object-backed modules retain their normal resource authorization.
	let body = { module: { kind: ts, referent: { node: $object, options: {} } } }
	let response = ($body | to json --raw)
		| http post --allow-errors --full --headers $headers $'($local.url)/modules/load'
	if $token == null {
		assert ($response.status != 200)
	} else {
		assert equal $response.status 200
		assert equal $response.body.text 'export default 42;'
	}

	# Labeling an arbitrary filesystem path as a declaration must not bypass the restriction.
	let body = { module: { kind: dts, referent: { node: $secret_path, options: {} } } }
	let response = ($body | to json --raw)
		| http post --allow-errors --full --headers $headers $'($local.url)/modules/load'
	assert ($response.status != 200)
}
