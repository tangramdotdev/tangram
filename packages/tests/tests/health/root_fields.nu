use ../../util/test.nu *

# Only root receives health fields when user authentication is enabled.

let root_token = random chars
let local = server spawn --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}

let anonymous = tg health --fields version | from json
assert equal $anonymous {} "anonymous callers should receive no health fields"

let user = tg login --verbose --name alice | from json
let health = tg --token $user.token health | from json
assert equal $health {} "authenticated users should receive no health fields"

let health = tg --token $root_token health --fields version | from json
assert equal ($health | columns) [version] "root should receive the requested field"
