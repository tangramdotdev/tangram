use ../lib/test.nu *

# A local nonrecursive list returns only the immediate children of its parent.

let local = server spawn
let artifact = artifact 'contents'
tg tag -p foo/bar $artifact
tg tag -p foo/baz/qux $artifact

let root = tg list --local | from json
assert equal ($root | get specifier) [foo]

let children = tg list --local foo | from json
assert equal ($children | get specifier) [foo/bar foo/baz]

let first = tg list --limit 1 --local foo --verbose | from json
let child = tg list --limit 1 --local --cursor $first.cursor foo | from json
assert equal ($child | get specifier) [foo/baz]

let first = tg list --limit 1 --local --reverse foo --verbose | from json
let child = tg list --limit 1 --local --cursor $first.cursor --reverse foo | from json
assert equal ($child | get specifier) [foo/bar]

let nested = tg list --local foo/baz | from json
assert equal ($nested | get specifier) [foo/baz/qux]

tg group create versions | ignore
tg group create versions/1.0.0 | ignore
tg tag versions/1.0.0/linux $artifact
tg group create versions/1.1.0 | ignore
tg tag versions/1.1.0/macos $artifact
let version = tg list --local "versions/^1" | from json
assert equal ($version | get specifier) [versions/1.1.0/macos]

# Pagination is applied after authorization filters hidden rows.
let local_auth = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $local_auth.url login --verbose --name alice | from json
let bob = tg --url $local_auth.url login --verbose --name bob | from json
tg --url $local_auth.url --token $alice.token group create a-hidden | ignore
tg --url $local_auth.url --token $alice.token group create b-visible | ignore
tg --url $local_auth.url --token $alice.token group create c-visible | ignore
tg --url $local_auth.url --token $alice.token grant $bob.user.id read b-visible | ignore
tg --url $local_auth.url --token $alice.token grant $bob.user.id read c-visible | ignore
tg --url $local_auth.url index
let first = tg --url $local_auth.url --token $bob.token list --limit 1 --local --no-organizations --no-tags --no-users --verbose | from json
let visible = (
	tg --url $local_auth.url --token $bob.token list --limit 1 --local --no-organizations --no-tags --no-users --cursor $first.cursor
	| from json
)
assert equal ($visible | get specifier) [c-visible]
