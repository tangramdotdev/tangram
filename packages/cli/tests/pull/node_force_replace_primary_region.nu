use ../lib/test.nu *

# Pulling with force through a secondary region replaces conflicting nodes in the primary region.

let local_source = server spawn --cloud --name local-source
let new_root = tg --url $local_source.url group create tree | from json
let new_child = tg --url $local_source.url group create tree/new | from json
tg --url $local_source.url index

let database_directory = mktemp -d
let database_path = $database_directory | path join 'database'
let primary_directory = mktemp -d
let secondary_directory = mktemp -d
let regions = [
	{ name: 'primary' },
	{ name: 'secondary' },
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
let remote_primary = server spawn --instance $instance --region primary --preserve-keys --name remote-primary --directory $primary_directory --url (instance region url $instance primary)
let remote_secondary = server spawn --instance $instance --region secondary --preserve-keys --name remote-secondary --directory $secondary_directory --url (instance region url $instance secondary)
tg --url $remote_secondary.url remote put default $local_source.url

let old_root = tg --url $remote_primary.url group create tree | from json
let old_child = tg --url $remote_primary.url group create tree/old | from json
tg --url $remote_primary.url index

tg --url $remote_secondary.url pull --force --group-children tree
tg --url $remote_primary.url index

assert equal (tg --url $remote_primary.url group get tree | from json | get id) $new_root.id
assert equal (tg --url $remote_primary.url group get tree/new | from json | get id) $new_child.id
failure (
	tg --url $remote_primary.url group get --location='local(primary)' $old_root.id | complete
) "the conflicting group should be deleted"
failure (
	tg --url $remote_primary.url group get --location='local(primary)' $old_child.id | complete
) "the conflicting descendant should be deleted"
