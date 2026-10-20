use ../lib/test.nu *

# A descendant with a partial capture can later gain permanent subtree access.
let root = random chars
let local = server spawn --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root }, users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let directory = tg --token $root put --no-tokens 'tg.directory({"nested":tg.directory({"file":tg.file("upgrade")})})' | referent node
let file = tg --token $root children $directory | from json | get 0 | referent node
let leaf = tg --token $root children $file | from json | get 0 | referent node
for resource in [$directory $file] {
	tg --token $root grant $alice.user.id object_node $resource | ignore
}
tg --token $root index
let params = { resource: $file } | to json --raw
let advanced = tg --token $root checkpoint watch permission_capture.advanced --params $params | from json | get watch
tg --token $alice.token tag put upgrade $directory
let tag = tg --token $alice.token tag get upgrade | from json
tg --token $alice.token grant $bob.user.id tag_read $tag.id | ignore
# Awaiting indexing must finish even while the capture worker is paused.
timeout 30s tg --token $root index
timeout 30s tg --token $root checkpoint wait permission_capture.advanced $advanced 0 | ignore
tg --token $root checkpoint unwatch permission_capture.advanced $advanced
failure (tg --token $bob.token get --bytes $leaf | complete) "node capture must not confer subtree access"

let written = tg --token $root checkpoint watch permission_capture.written --params $params | from json | get watch
tg --token $root grant $alice.user.id object_subtree $file | ignore
tg --token $root index
timeout 30s tg --token $root checkpoint wait permission_capture.written $written 0 | ignore
tg --token $root checkpoint unwatch permission_capture.written $written
tg --token $root revoke $alice.user.id object_subtree $file | ignore
for resource in [$directory $file] {
	tg --token $root revoke $alice.user.id object_node $resource | ignore
}
tg --token $root index
success (tg --token $bob.token get --bytes $leaf | complete) "the descendant upgrade must survive revoking its source permissions"
