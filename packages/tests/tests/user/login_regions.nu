use ../../util/test.nu *

# Explicit regions choose where a database-backed request executes.
let common = {
	authentication: { users: { providers: { insecure: true } } },
	database: { index_queue: { wakeup_interval: 0.01 } },
}
let instance = instance --cloud --primary-region a --regions [{ name: a }, { name: b }] --config $common
let region_a = server spawn --preserve-keys --instance $instance --region a --name region-a --url (instance region url $instance a)
let region_b = server spawn --preserve-keys --instance $instance --region b --name region-b --url (instance region url $instance b) --config { authentication: { users: null } }

# An unspecified region retains primary-region login routing.
let alice = tg --url $region_b.url login --verbose --name alice | from json
assert equal $alice.user.specifier alice
let bob = tg --url $region_b.url login --location='local(a)' --verbose --name bob | from json
assert equal $bob.user.specifier bob

# Region B has no login providers, even when the receiving server is region A.
for url in [$region_a.url $region_b.url] {
	let output = tg --url $url login --location='local(b)' --name carol | complete
	failure $output
	assert ($output.stderr | str contains "no authentication providers are configured")
}

let local = server spawn --name local --config {
	remotes: { default: { url: $region_a.url } },
}
let output = tg login --location='remote(b)' --name carol | complete
failure $output
assert ($output.stderr | str contains "no authentication providers are configured")
let remote_user = tg login --location='remote(a)' --name carol | from json
assert equal $remote_user.specifier carol

failure (tg --url $region_a.url login --location='local(missing)' --name unknown | complete)
