use ../../lib/test.nu *

# Default usage totals include each regional index exactly once.
def sum_usage [left: record, right: record] {
	mut output = $left
	for field in [object_count object_size process_count sandbox_count sandbox_cpu_consumption sandbox_cpu_reservation sandbox_memory_consumption sandbox_memory_reservation sandbox_storage_consumption sandbox_storage_reservation] {
		$output = $output | update $field (($left | get $field) + ($right | get $field))
	}
	$output
}

let root_token = random chars
let common = {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	database: { index_queue: { wakeup_interval: 0.01 } },
	usage: true,
}
let instance = instance --cloud --primary-region a --regions [{ name: a }, { name: b }] --config $common
let region_a = server spawn --preserve-keys --now 2026-08-11T12:00:00Z --instance $instance --region a --name region-a --url (instance region url $instance a)
let region_b = server spawn --preserve-keys --now 2026-08-11T12:00:00Z --instance $instance --region b --name region-b --url (instance region url $instance b)
let alice = tg --url $region_a.url login --verbose --name alice | from json
let organization = tg --url $region_a.url --token $alice.token organization create acme | from json
let object_a = tg --url $region_a.url --token $alice.token put 'tg.file("a")' | str trim
let object_b = tg --url $region_b.url --token $alice.token put 'tg.file("a larger regional object")' | str trim
tg --url $region_a.url --token $alice.token tag put acme/a $object_a
tg --url $region_b.url --token $alice.token tag put --location='local(b)' acme/b $object_b
tg --url $region_a.url --token $root_token index --usage
tg --url $region_b.url --token $root_token index --usage
let count_a = tg --url $region_a.url --token $alice.token organization usage --location='local(a)' acme | from json | get object_count
let count_b = tg --url $region_b.url --token $alice.token organization usage --location='local(b)' acme | from json | get object_count
assert ($count_a >= 1 and $count_b >= 1) "both regions should record the organization's storage"

let local = server spawn --name local --config {
	remotes: {
		default: { url: $region_a.url, token: $alice.token },
		staging: { url: $region_b.url, token: $alice.token },
	},
}

for selector in [$alice.user.id $organization.id] {
	let usage_a = tg --url $region_a.url --token $alice.token usage --location='local(a)' $selector | from json
	let usage_b = tg --url $region_b.url --token $alice.token usage --location='local(b)' $selector | from json
	assert ($usage_a.object_count > 0)
	assert ($usage_b.object_count > 0)
	assert ($usage_a.object_size != $usage_b.object_size)
	let total = sum_usage $usage_a $usage_b

	assert equal (tg --url $region_a.url --token $alice.token usage $selector | from json) $total
	assert equal (tg --url $region_b.url --token $alice.token usage $selector | from json) $total
	assert equal (tg usage -r $selector | from json) $total
	assert equal (tg usage -r=staging $selector | from json) $total
	assert equal (tg usage -r --month 2026-08 $selector | from json) $total
	assert equal (tg usage --location='remote(b)' $selector | from json) $usage_b
	assert equal (tg usage --location='remote:staging(a)' $selector | from json) $usage_a
}

let user = tg usage -r alice | from json
assert equal (tg usage -r | from json) $user
assert equal (tg user usage -r | from json) $user
assert equal (tg organization usage -r acme | from json) (tg usage -r acme | from json)
