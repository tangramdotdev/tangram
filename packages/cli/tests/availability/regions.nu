use ../lib/test.nu *

skip_if_no_cloud

# Availability can be requested from a specific peer region.

let regions = [
	{ name: 'a' },
	{ name: 'b' },
]
let instance = instance --cloud --primary-region a --regions $regions
let remote_region_a = server spawn --instance $instance --region a --name remote-region-a --url (instance region url $instance a)
let remote_region_b = server spawn --instance $instance --region b --name remote-region-b --url (instance region url $instance b)

let directory = tg --url $remote_region_a.url put --no-tokens 'tg.directory({ "file": tg.file("contents") })' | referent node
tg --url $remote_region_a.url index

let availability = tg --url $remote_region_b.url object availability $directory --location='local(a)' | from json
assert equal $availability.subtree true "the peer region should report that the object subtree is available"

let local = tg --url $remote_region_b.url object availability $directory --location='local(b)' | complete
failure $local "the object's availability should be absent from the current region"
