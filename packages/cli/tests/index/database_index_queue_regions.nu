use ../lib/test.nu *

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
	database: {
		kind: 'sqlite',
		index_queue: { wakeup_interval: 0.01 },
		path: $database_path,
	},
}
let instance = instance --primary-region east --regions $regions --config $common
let producer = { roles: [api runner scheduler] }
let remote_east = server spawn --instance $instance --region east --name remote-east --directory $east_directory --url (instance region url $instance east) --config $producer
let remote_west = server spawn --instance $instance --region west --name remote-west --directory $west_directory --url (instance region url $instance west) --config $producer

let east_group = tg --url $remote_east.url group create east-project | from json
let west_group = tg --url $remote_west.url group create west-project | from json
let rows = (
	open $database_path
	| query db 'select region, batch from index_queue order by batch, region'
)
assert equal ($rows | get batch) [1 1 2 2]
assert equal ($rows | get region) [east west east west]
let next = open $database_path | query db 'select next from index_queue_batch' | get next.0
assert equal $next 2

server stop $remote_east
server stop $remote_west

let remote_east = server spawn --instance $instance --region east --name remote-east --directory $east_directory --url (instance region url $instance east)
let remote_west = server spawn --instance $instance --region west --name remote-west --directory $west_directory --url (instance region url $instance west)
tg --url $remote_east.url index
tg --url $remote_west.url index

let indexed = tg --url $remote_west.url group get east-project | from json
assert equal $indexed.id $east_group.id
let indexed = tg --url $remote_east.url group get west-project | from json
assert equal $indexed.id $west_group.id
