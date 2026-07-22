use ../../test.nu *

# Database index batches are fanned out to every configured region.

let database_directory = mktemp -d
let database_path = $database_directory | path join 'database'
let east_directory = mktemp -d
let west_directory = mktemp -d
let east_url = $'http+unix://($east_directory | url encode --all)%2Fsocket'
let west_url = $'http+unix://($west_directory | url encode --all)%2Fsocket'
let regions = [
	{ name: 'east', url: $east_url },
	{ name: 'west', url: $west_url },
]
let common = {
	database: { kind: 'sqlite', path: $database_path },
	regions: $regions,
}
let east = spawn --name east --directory $east_directory --url $east_url --config ($common | merge { region: 'east' })
let west = spawn --name west --directory $west_directory --url $west_url --config ($common | merge { region: 'west' })

let group = tg --url $east.url group create project | from json
tg --url $east.url index
tg --url $west.url index

let indexed = tg --url $west.url group get project | from json
assert equal $indexed.id $group.id
