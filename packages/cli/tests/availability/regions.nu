use ../lib/test.nu *

# Availability can be requested from a specific peer region.

let region_a_directory = mktemp -d
let region_b_directory = mktemp -d
let database_directory = mktemp -d
let database_path = $database_directory | path join 'database'
let regions = [
	{ name: 'a' },
	{ name: 'b' },
]
let common = {
	database: { kind: 'sqlite', path: $database_path },
}
let instance = instance --primary-region a --regions $regions --config $common
let remote_region_a = server spawn --instance $instance --region a --name remote-region-a --directory $region_a_directory --url (instance region url $instance a)
let remote_region_b = server spawn --instance $instance --region b --name remote-region-b --directory $region_b_directory --url (instance region url $instance b)

let directory = tg --url $remote_region_a.url put --no-tokens 'tg.directory({ "file": tg.file("contents") })' | referent node
tg --url $remote_region_a.url index

let availability = tg --url $remote_region_b.url object availability $directory --location='local(a)' | from json
assert equal $availability.subtree true "the peer region should report that the object subtree is available"

let local = tg --url $remote_region_b.url object availability $directory --location='local(b)' | complete
failure $local "the object's availability should be absent from the current region"
