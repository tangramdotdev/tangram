use ../lib/test.nu *

# A user on a shared server must not inherit the server's configured service remote. The server-level remote and its token are isolated from authenticated users, so a user cannot ride the server's credentials to pull a private object from the source.

let local_source = server spawn --cloud --name local-source --config { authentication: { users: { providers: { insecure: true } } } }

let alice_s = tg --url $local_source.url login --verbose --name alice | from json

# Alice stores a private file on the source.
let file = tg --url $local_source.url --token $alice_s.token put --no-tokens 'tg.file("topsecret")' | referent node
tg --url $local_source.url index

# A shared server reaches the source via Alice's source token as its server-level remote.
let remote_shared = server spawn --name remote-shared --config {
	authentication: { users: { providers: { insecure: true } } },
	remotes: { default: { url: $local_source.url, token: $alice_s.token } },
}

let eve_b = tg --url $remote_shared.url login --verbose --name eve | from json

# Eve does not inherit the server-level remote.
let eve_remotes = tg --url $remote_shared.url --token $eve_b.token remote list | from json
assert equal $eve_remotes [] "a user must not inherit the server-level remote."

# Eve cannot pull the source-private file by riding the server's configured remote.
let pulled = tg --url $remote_shared.url --token $eve_b.token pull $file | complete
failure $pulled "Eve must not pull through a remote she does not have."

# Eve still cannot read Alice's private file on the shared server.
let leaked = tg --url $remote_shared.url --token $eve_b.token get $file | complete
failure $leaked "Eve must not obtain the source-private object through the shared server."
