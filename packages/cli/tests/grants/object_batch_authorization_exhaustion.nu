use ../lib/test.nu *

let root_token = random chars
let local = server spawn --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	verification: { permissions: { initial: false, final: false } },
}
let bob = tg login --verbose --name bob | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let child = tg --token $root_token put --no-tokens 'tg.file("private")' | referent node
let parent = tg --token $root_token put --no-tokens ('tg.directory({ "value": CHILD })' | str replace CHILD $child) | referent node
let unrelated = tg --token $bob.token put --no-tokens 'tg.blob("unrelated")' | referent node
let unrelated = http get --headers { Accept: 'application/json', Authorization: $'Bearer ($root_token)' } --unix-socket $socket $'http://localhost/objects/($unrelated)'
let token = $unrelated.tokens.local.0
let child_referent = $'($child)?tokens[local][0]=($token | url encode --all)'
let object = {
	id: $parent,
	data: { kind: directory, value: { entries: { value: $child } } },
	children: [$child_referent],
}

# Optional subtree authorization must not prevent storing the supplied parent node.
let output = http post --max-time 10sec --content-type application/json --headers { Authorization: $'Bearer ($bob.token)' } --unix-socket $socket http://localhost/objects/batch ({ objects: [$object] } | to json --raw) | from json
let token = $'http://localhost/($output.objects.0)' | url parse | get params | where key == 'tokens[local][0]' | first | get value
let body = $token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
assert equal $body.permissions [object_node]
success (tg --token $bob.token get --depth 1 $output.objects.0 | complete) 'Bob should be able to read the parent node'
failure (tg --token $bob.token get $child | complete) 'storing the parent must not authorize its private child'
