use ../lib/test.nu *

# A user-configured remote carries no server credentials. A user on a shared server may add her own remote to a source, but her requests authenticate anonymously, so she still cannot pull a private object she has no access to on the source.

let local_source = server spawn --cloud --name local-source --config { authentication: { users: { providers: { insecure: true } } } }

let alice_s = tg --url $local_source.url login --verbose --name alice | from json

# Alice stores a private file on the source.
let file = tg --url $local_source.url --token $alice_s.token put --no-tokens 'tg.file("topsecret")' | referent node
tg --url $local_source.url index

# A shared server whose server-level remote points at the source via Alice's token.
let remote_shared = server spawn --name remote-shared --config {
	authentication: { users: { providers: { insecure: true } } },
	remotes: { default: { url: $local_source.url, token: $alice_s.token } },
}

# Eve, a user on the shared server, adds her own remote to the source.
let eve_b = tg --url $remote_shared.url login --verbose --name eve | from json
tg --url $remote_shared.url --token $eve_b.token remote put default $local_source.url

# Eve's own remote carries no credential, so her pull authenticates anonymously and the source denies the private file.
let pulled = tg --url $remote_shared.url --token $eve_b.token pull $file | complete
failure $pulled "a user-configured remote must not pull a private source object, since it authenticates anonymously."

# Eve still cannot read the file on the shared server.
let leaked = tg --url $remote_shared.url --token $eve_b.token get $file | complete
failure $leaked "Eve must not obtain the source-private object through her own remote."
