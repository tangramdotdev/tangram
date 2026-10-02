use ../lib/test.nu *

# Usage forwards account selection and period selectors to the requested remote.
let remote = server spawn --now 2026-08-11T12:00:00Z --config {
	authentication: { users: { providers: { insecure: true } } },
	usage: true,
}
let alice = tg login --verbose --name alice | from json
let organization = tg organization create acme | from json
let user_usage = tg usage | from json
let organization_usage = tg usage acme | from json
let month = $user_usage.period.start | str substring 0..6

# The forwarding server has no local user and has usage tracking disabled.
let local = server spawn --config {
	remotes: {
		default: { url: $remote.url, token: $alice.token },
		staging: { url: $remote.url, token: $alice.token },
	},
}

assert equal (tg usage -r | from json) $user_usage
assert equal (tg usage -r=staging | from json) $user_usage
assert equal (tg usage -r alice | from json) $user_usage
assert equal (tg usage -r $alice.user.id | from json) $user_usage
assert equal (tg user usage -r | from json) $user_usage
assert equal (tg usage -r --month $month | from json) $user_usage
assert equal (tg user usage -r=staging --month $month | from json) $user_usage

assert equal (tg usage -r acme | from json) $organization_usage
assert equal (tg usage -r $organization.id | from json) $organization_usage
assert equal (tg organization usage -r acme | from json) $organization_usage
assert equal (tg organization usage -r=staging --month $month $organization.id | from json) $organization_usage

let output = tg usage --local $alice.user.id | complete
failure $output
assert ($output.stderr | str contains "usage tracking is disabled")
