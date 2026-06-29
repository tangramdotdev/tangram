use ../../test.nu *

# A user-configured remote carries no server credentials. A user on a shared server may add her own remote to a source, but her requests authenticate anonymously, so she still cannot pull a private object she has no access to on the source.

let source = spawn --cloud --name source --config { authentication: { providers: { insecure: true } } }

let alice_s = tg --url $source.url login --verbose alice | from json

# Alice stores a private file on the source.
let file = tg --url $source.url --token $alice_s.token put 'tg.file("topsecret")' | str trim
tg --url $source.url index

# A shared server whose server-level remote points at the source via Alice's token.
let shared = spawn --name shared --config {
	authentication: { providers: { insecure: true } },
	remotes: { default: { url: $source.url, token: $alice_s.token } },
}

# Eve, a user on the shared server, adds her own remote to the source.
let eve_b = tg --url $shared.url login --verbose eve | from json
tg --url $shared.url --token $eve_b.token remote put default $source.url

# Eve's own remote carries no credential, so her pull authenticates anonymously and the source denies the private file.
let pulled = tg --url $shared.url --token $eve_b.token pull $file | complete
failure $pulled "a user-configured remote must not pull a private source object, since it authenticates anonymously."

# Eve still cannot read the file on the shared server.
let leaked = tg --url $shared.url --token $eve_b.token get $file | complete
failure $leaked "Eve must not obtain the source-private object through her own remote."
