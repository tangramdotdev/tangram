use ../lib/test.nu *

skip_if_no_cloud

# Database index batches are fanned out to every configured region.

let regions = [
	{ name: 'east' },
	{ name: 'west' },
]
let common = {
	database: { index_queue: { wakeup_interval: 0.01 } },
}
let instance = instance --cloud --primary-region east --regions $regions --config $common
let producer = { roles: [api runner scheduler] }
let remote_east = server spawn --instance $instance --region east --name remote-east --url (instance region url $instance east) --config $producer
let remote_west = server spawn --instance $instance --region west --name remote-west --url (instance region url $instance west) --config $producer

let east_group = tg --url $remote_east.url group create east-project | from json
let west_group = tg --url $remote_west.url group create west-project | from json
let rows = (
	psql $remote_east.config.database.write.url --no-psqlrc --set=ON_ERROR_STOP=1 --csv --command 'select region, batch from index_queue order by batch, region'
	| from csv
	| update batch { into int }
)
assert equal ($rows | get batch) [1 1 2 2]
assert equal ($rows | get region) [east west east west]
let next = psql $remote_east.config.database.write.url --no-psqlrc --set=ON_ERROR_STOP=1 --tuples-only --no-align --command 'select next from index_queue_batch' | str trim | into int
assert equal $next 2

server stop $remote_east
server stop $remote_west

let remote_east = server spawn --instance $instance --region east --name remote-east --directory $remote_east.directory --url (instance region url $instance east)
let remote_west = server spawn --instance $instance --region west --name remote-west --directory $remote_west.directory --url (instance region url $instance west)
tg --url $remote_east.url index
tg --url $remote_west.url index

let indexed = tg --url $remote_west.url group get east-project | from json
assert equal $indexed.id $east_group.id
let indexed = tg --url $remote_east.url group get west-project | from json
assert equal $indexed.id $west_group.id
