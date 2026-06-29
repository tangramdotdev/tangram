use ../../test.nu *

# Publishing a --public tag to a remote is bounded by the publisher's access there, not the forwarding server's root: a user who cannot read a registry object must not make it readable by forwarding a --public tag that names it. The remote recomputes the recorded permissions against the publisher.

let registry = spawn --cloud --name registry --config { authentication: { providers: { insecure: true } } }

let carol = tg --url $registry.url login --verbose carol | from json
let alice = tg --url $registry.url login --verbose alice | from json
let eve = tg --url $registry.url login --verbose eve | from json

# Carol stores a private object on the registry.
let secret = tg --url $registry.url --token $carol.token put 'tg.file("carolsecret")' | str trim
tg --url $registry.url index

# Alice cannot read Carol's private object on the registry.
let alice_denied = tg --url $registry.url --token $alice.token get $secret | complete
failure $alice_denied "Alice should not read Carol's private object before the exploit."

# Alice has her own local server that talks to the registry as Alice; on it she is root.
let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $registry.url, token: $alice.token } },
}

# Alice publishes a --public tag naming Carol's object, forwarding the put to the registry.
let put = tg --url $alice_local.url tag put evil $secret --public --remote | complete
success $put "Alice should create her own public tag."
tg --url $registry.url index

# Eve resolves the public tag, proving the publish reached the registry.
let resolved = tg --url $registry.url --token $eve.token tag get evil | complete
success $resolved "Eve should resolve Alice's public tag."

# But Eve must not read Carol's object through it: the registry recorded Alice's access (none), not the local root's.
let eve_read = tg --url $registry.url --token $eve.token get $secret | complete
failure $eve_read "a forwarded public tag must not confer an object the publisher cannot read."
let anon_read = tg --url $registry.url get $secret | complete
failure $anon_read "an anonymous client must not read the object through the forwarded public tag."
