use ../lib/test.nu *

# Logout preserves the explicit region when the receiving region has account management disabled.
let region_a_directory = mktemp -d
let region_b_directory = mktemp -d
let database_path = mktemp -d | path join database
let common = {
	authentication: { users: { providers: { insecure: true } } },
	database: { kind: sqlite, index_queue: { wakeup_interval: 0.01 }, path: $database_path },
}
let instance = instance --primary-region b --regions [{ name: a }, { name: b }] --config $common
let region_a = server spawn --preserve-keys --instance $instance --region a --name region-a --directory $region_a_directory --url (instance region url $instance a) --config { accounts: false }
let region_b = server spawn --preserve-keys --instance $instance --region b --name region-b --directory $region_b_directory --url (instance region url $instance b)
let alice = tg --url $region_b.url login --verbose --name alice | from json

assert equal (tg --url $region_a.url whoami --location='local(b)' | from json).id $alice.user.id
tg --url $region_a.url logout --location='local(b)'
failure (tg --url $region_b.url --token $alice.token whoami --local | complete) "the selected region's session must be revoked"
failure (tg --url $region_b.url whoami --local | complete) "the local CLI token must be cleared"
