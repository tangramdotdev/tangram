use ../../test.nu *

# A local nonrecursive list returns only the immediate children of its parent.

let server = server spawn
let artifact = artifact 'contents'
tg tag -p foo/bar $artifact
tg tag -p foo/baz/qux $artifact

let root = tg list --local | from json
assert equal ($root | get specifier) [foo]

let children = tg list --local foo | from json
assert equal ($children | get specifier) [foo/bar foo/baz]

let child = tg list --length 1 --local --position 1 foo | from json
assert equal ($child | get specifier) [foo/baz]

let child = tg list --length 1 --local --position 1 --reverse foo | from json
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
let auth = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $auth.url login --verbose --name alice | from json
let bob = tg --url $auth.url login --verbose --name bob | from json
tg --url $auth.url --token $alice.token group create a-hidden | ignore
tg --url $auth.url --token $alice.token group create b-visible | ignore
tg --url $auth.url --token $alice.token group create c-visible | ignore
tg --url $auth.url --token $alice.token grant $bob.user.id read b-visible | ignore
tg --url $auth.url --token $alice.token grant $bob.user.id read c-visible | ignore
tg --url $auth.url index
let visible = (
	tg --url $auth.url --token $bob.token list --length 1 --local --no-organizations --no-tags --no-users --position 1
	| from json
)
assert equal ($visible | get specifier) [c-visible]
