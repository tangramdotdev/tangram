use ../../test.nu *

# A user on a shared server must not inherit the server's configured service remote. The server-level remote and its token are isolated from authenticated users, so a user cannot ride the server's credentials to pull a private object from the source.

let source = spawn --cloud --name source --config { authentication: { providers: { insecure: true } } }

let alice_s = tg --url $source.url login --verbose alice | from json

# Alice stores a private file on the source.
let file = tg --url $source.url --token $alice_s.token put 'tg.file("topsecret")' | str trim
tg --url $source.url index

# A shared server reaches the source via Alice's source token as its server-level remote.
let shared = spawn --name shared --config {
	authentication: { providers: { insecure: true } },
	remotes: { default: { url: $source.url, token: $alice_s.token } },
}

let eve_b = tg --url $shared.url login --verbose eve | from json

# Eve does not inherit the server-level remote.
let eve_remotes = tg --url $shared.url --token $eve_b.token remote list | from json
assert equal $eve_remotes [] "a user must not inherit the server-level remote."

# Eve cannot pull the source-private file by riding the server's configured remote.
let pulled = tg --url $shared.url --token $eve_b.token pull $file | complete
failure $pulled "Eve must not pull through a remote she does not have."

# Eve still cannot read Alice's private file on the shared server.
let leaked = tg --url $shared.url --token $eve_b.token get $file | complete
failure $leaked "Eve must not obtain the source-private object through the shared server."
