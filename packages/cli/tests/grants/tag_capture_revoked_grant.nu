use ../lib/test.nu *

# A tag must not keep a creator's access to its target after the grant that provided it is revoked.

let root_token = random chars
let local = server spawn --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let reader = tg login --verbose --name reader | from json

# Bob owns the directory and grants Alice read on it.
let directory = tg --token $bob.token put --no-tokens 'tg.directory({"file":tg.file("revoked")})' | referent node
tg --token $root_token index
failure (tg --token $alice.token get --bytes $directory | complete) "Alice must have no access before the grant."
tg --token $bob.token grant $alice.user.id object_subtree $directory | ignore
tg --token $root_token index
success (tg --token $alice.token get --bytes $directory | complete) "Alice must read the directory while the grant exists."

# Alice tags the directory while she holds the grant.
tg --token $alice.token tag put foo $directory
tg --token $root_token index
let tag = tg --token $alice.token tag get foo | from json
tg --token $alice.token grant $reader.user.id tag_read $tag.id | ignore
tg --token $root_token index

# Bob revokes the grant.
tg --token $bob.token revoke $alice.user.id object_subtree $directory
tg --token $root_token index
failure (tg --token $alice.token get --bytes $directory | complete) "Alice must lose access to the directory when the grant is revoked."
failure (tg --token $reader.token get --bytes $directory | complete) "a tag reader must not read the directory after the creator's grant is revoked."
