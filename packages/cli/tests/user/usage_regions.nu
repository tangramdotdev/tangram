use ../lib/test.nu *

# Explicit region selection reads that region's usage through local and remote routing.
let region_a_directory = mktemp -d
let region_b_directory = mktemp -d
let database_path = mktemp -d | path join database
let common = {
	authentication: { users: { providers: { insecure: true } } },
	database: { kind: sqlite, index_queue: { wakeup_interval: 0.01 }, path: $database_path },
	usage: true,
}
let instance = instance --primary-region a --regions [{ name: a }, { name: b }] --config $common
let region_a = server spawn --preserve-keys --instance $instance --region a --name region-a --directory $region_a_directory --url (instance region url $instance a) --config { usage: false }
let region_b = server spawn --preserve-keys --instance $instance --region b --name region-b --directory $region_b_directory --url (instance region url $instance b)
let alice = tg --url $region_a.url login --verbose --name alice | from json
let organization = tg --url $region_a.url --token $alice.token organization create acme | from json
let user_usage = tg --url $region_b.url --token $alice.token usage --location='local(b)' | from json
let organization_usage = tg --url $region_b.url --token $alice.token organization usage --location='local(b)' acme | from json

assert equal (tg --url $region_a.url --token $alice.token usage --location='local(b)' $alice.user.id | from json) $user_usage
assert equal (tg --url $region_a.url --token $alice.token user usage --location='local(b)' | from json) $user_usage
assert equal (tg --url $region_a.url --token $alice.token organization usage --location='local(b)' $organization.id | from json) $organization_usage

let local = server spawn --name local --config {
	remotes: { default: { url: $region_a.url, token: $alice.token } },
}
assert equal (tg usage --location='remote(b)' alice | from json) $user_usage
assert equal (tg user usage --location='remote(b)' | from json) $user_usage
assert equal (tg usage --location='remote(b)' acme | from json) $organization_usage
assert equal (tg organization usage --location='remote(b)' $organization.id | from json) $organization_usage

# Selecting the disabled region must not read usage from the receiving region.
let output = tg --url $region_b.url --token $alice.token usage --location='local(a)' $alice.user.id | complete
failure $output
assert ($output.stderr | str contains "usage tracking is disabled")
let output = tg --url $region_b.url --token $alice.token organization usage --location='local(a)' acme | complete
failure $output
assert ($output.stderr | str contains "usage tracking is disabled")

# Aggregation fails if any region cannot report usage.
failure (tg --url $region_b.url --token $alice.token usage | complete)
failure (tg --url $region_b.url --token $alice.token organization usage acme | complete)
failure (tg usage -r | complete)
failure (tg organization usage -r acme | complete)
