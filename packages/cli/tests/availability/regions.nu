use ../../test.nu *

# Availability can be requested from a specific peer region.

let region_a_directory = mktemp -d
let region_b_directory = mktemp -d
let database_directory = mktemp -d
let database_path = $database_directory | path join 'database'
let region_a_url = $'http+unix://($region_a_directory | url encode --all)%2Fsocket'
let region_b_url = $'http+unix://($region_b_directory | url encode --all)%2Fsocket'
let regions = [
	{ name: 'a', url: $region_a_url },
	{ name: 'b', url: $region_b_url },
]
let common = {
	database: { kind: 'sqlite', path: $database_path },
	primary_region: 'a',
	regions: $regions,
}
let region_a = spawn --name region-a --directory $region_a_directory --url $region_a_url --config ($common | merge { region: 'a' })
let region_b = spawn --name region-b --directory $region_b_directory --url $region_b_url --config ($common | merge { region: 'b' })

let directory = tg --url $region_a.url put 'tg.directory({ "file": tg.file("contents") })' | str trim
tg --url $region_a.url index

let availability = tg --url $region_b.url object availability $directory --location='local(a)' | from json
assert equal $availability.subtree true "the peer region should report that the object subtree is available"

let local = tg --url $region_b.url object availability $directory --location='local(b)' | complete
failure $local "the object's availability should be absent from the current region"
