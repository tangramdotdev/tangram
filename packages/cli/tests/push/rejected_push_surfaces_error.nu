use ../../test.nu *

# A push the remote rejects fails with the remote's error instead of hanging or reporting a generic sync failure.

let remote = spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

# The local server has the user but no token for the remote, so the remote rejects the sync.
let local = spawn --name local --config {
	authentication: { users: { providers: { insecure: true } } },
	remotes: { default: { url: $remote.url } },
}
tg --url $local.url login --verbose alice | from json

let id = tg --url $local.url put 'tg.file("hello")' | str trim
tg --url $local.url tag put alice/example $id

let output = (tg --url $local.url --no-quiet push alice/example | complete)
failure $output "a push the remote rejects should fail"
