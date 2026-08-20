use ../../test.nu *

# Pulling an object through a remote region fetches it from a peer region when it is absent in the region serving the request.

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
let local = spawn --name local --config {
	remotes: {
		default: {
			url: $region_b.url,
		},
	},
}

# Put a directory and its subtree in region A only.
let directory = tg --url $region_a.url put 'tg.directory({ "file": tg.file("contents") })' | str trim
let file = tg --url $region_a.url children $directory | from json | get 0
let blob = tg --url $region_a.url children $file | from json | get 0

# Confirm that region B and the local server do not have the objects.
for id in [$directory $file $blob] {
	let region_b_object = tg --url $region_b.url object get --bytes --location='local(b)' $id | complete
	failure $region_b_object "the object should be absent in region B before the pull"
	let local_object = tg --url $local.url object get --bytes --local $id | complete
	failure $local_object "the object should be absent locally before the pull"
}

# Pull through region B, which should fetch the objects from region A.
let output = tg --url $local.url pull $directory | complete
success $output "the pull should succeed through a region that does not have the object"

# Confirm that the directory and its subtree were pulled.
for id in [$directory $file $blob] {
	let local_object = tg --url $local.url object get --bytes --local $id | complete
	success $local_object "the object should be present locally after the pull"
}
