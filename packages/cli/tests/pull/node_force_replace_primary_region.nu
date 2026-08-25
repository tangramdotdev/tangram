use ../../test.nu *

# Pulling with force through a secondary region replaces conflicting nodes in the primary region.

let source = server spawn --cloud --name source
let new_root = tg --url $source.url group create tree | from json
let new_child = tg --url $source.url group create tree/new | from json

let database_directory = mktemp -d
let database_path = $database_directory | path join 'database'
let primary_directory = mktemp -d
let secondary_directory = mktemp -d
let primary_url = $'http+unix://($primary_directory | url encode --all)%2Fsocket'
let secondary_url = $'http+unix://($secondary_directory | url encode --all)%2Fsocket'
let regions = [
	{ name: 'primary', url: $primary_url },
	{ name: 'secondary', url: $secondary_url },
]
let common = {
	advanced: {
		single_directory: false,
		single_process: false,
	},
	checkouts: false,
	database: { kind: 'sqlite', path: $database_path },
}
let instance = instance --primary-region primary --regions $regions --config $common
let primary = server spawn --instance $instance --region primary --preserve-keys --name primary --directory $primary_directory --url $primary_url
let secondary = server spawn --instance $instance --region secondary --preserve-keys --name secondary --directory $secondary_directory --url $secondary_url
tg --url $secondary.url remote put default $source.url

let old_root = tg --url $primary.url group create tree | from json
let old_child = tg --url $primary.url group create tree/old | from json
tg --url $primary.url index

tg --url $secondary.url pull --force --group-children tree

assert equal (tg --url $primary.url group get tree | from json | get id) $new_root.id
assert equal (tg --url $primary.url group get tree/new | from json | get id) $new_child.id
failure (
	tg --url $primary.url group get --location='local(primary)' $old_root.id | complete
) "the conflicting group should be deleted"
failure (
	tg --url $primary.url group get --location='local(primary)' $old_child.id | complete
) "the conflicting descendant should be deleted"
