use ../../test.nu *

# Database index batches are fanned out to every configured region.

let database_directory = mktemp -d
let database_path = $database_directory | path join 'database'
let east_directory = mktemp -d
let west_directory = mktemp -d
let regions = [
	{ name: 'east' },
	{ name: 'west' },
]
let common = {
	database: { kind: 'sqlite', path: $database_path },
	indexer: { database_index_outbox_wakeup_interval: 0.01 },
}
let instance = instance --primary-region east --regions $regions --config $common
let producer = { roles: [http runner scheduler] }
let east = server spawn --instance $instance --region east --name east --directory $east_directory --url (instance region url $instance east) --config $producer
let west = server spawn --instance $instance --region west --name west --directory $west_directory --url (instance region url $instance west) --config $producer

let east_group = tg --url $east.url group create east-project | from json
let west_group = tg --url $west.url group create west-project | from json
let rows = (
	open $database_path
	| query db 'select region, batch from index_outbox order by batch, region'
)
assert equal ($rows | get batch) [1 1 2 2]
assert equal ($rows | get region) [east west east west]
let next = open $database_path | query db 'select next from index_outbox_batch' | get next.0
assert equal $next 2

server stop $east
server stop $west

let east = server spawn --instance $instance --region east --name east-indexer --directory $east_directory --url (instance region url $instance east)
let west = server spawn --instance $instance --region west --name west-indexer --directory $west_directory --url (instance region url $instance west)
tg --url $east.url index
tg --url $west.url index

let indexed = tg --url $west.url group get east-project | from json
assert equal $indexed.id $east_group.id
let indexed = tg --url $east.url group get west-project | from json
assert equal $indexed.id $west_group.id
