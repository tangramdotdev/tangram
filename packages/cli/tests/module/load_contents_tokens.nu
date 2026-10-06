use ../lib/test.nu *

let root_token = random chars
let local = server spawn --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let text = 'export default () => 1;'
let file = tg --token $root_token put --no-tokens 'tg.file("export default () => 1;")' | referent node
let graph = tg --token $root_token put --no-tokens 'tg.graph({ "nodes": [{ "kind": "file", "contents": tg.blob("export default () => 1;") }] })' | referent node
let object = http get --headers { Accept: 'application/json', Authorization: $'Bearer ($root_token)' } --unix-socket $socket $'http://localhost/objects/($file)'
let blob = $object.data.value.contents
tg --token $root_token grant $alice.user.id object_node $file | ignore
tg --token $root_token grant $alice.user.id object_node $graph | ignore
tg --token $root_token grant $alice.user.id object_subtree $blob | ignore

for case in [
	{ node: $file, checkout: false },
	{ node: $'graph=($graph)&index=0&kind=file', checkout: false },
	{ node: $file, checkout: true },
] {
	if $case.checkout {
		tg --token $root_token checkout $file | ignore
	}
	let node = $case.node
	let module = { kind: typescript, referent: { node: $node, options: {} } }
	let first = http post --max-time 10sec --content-type application/json --headers { Authorization: $'Bearer ($alice.token)' } --unix-socket $socket http://localhost/modules/load ({ module: $module } | to json --raw)
	assert equal $first.text $text

	# Loading the module must retain authorization for its contents as well as the file or graph node.
	let module = $module | update referent.options { tokens: $first.tokens }
	let second = http post --max-time 10sec --content-type application/json --headers { Authorization: $'Bearer ($bob.token)' } --unix-socket $socket http://localhost/modules/load ({ module: $module } | to json --raw)
	assert equal $second.text $text
}
