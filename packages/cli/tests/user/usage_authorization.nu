use ../../test.nu *

# Usage requires account administration and missing selectors fail cleanly.

let server = spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	usage: true,
}
let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let organization = tg --token $alice.token organization create acme | from json

failure (tg --token $bob.token usage $alice.user.id | complete) "another user's usage must be private"
failure (tg --token $bob.token usage $organization.id | complete) "organization usage requires administration"
failure (tg --token $alice.token usage missing | complete) "a missing account should fail"

# Failed lookups do not affect subsequent authorized requests.
success (tg --token $alice.token usage $alice.user.id | complete)
success (tg --token $alice.token usage $organization.id | complete)
