use ../../test.nu *

# Database index batches are fanned out to every configured region.

def stop [server: record] {
	let pid = open ($server.directory | path join 'lock') | into int
	kill --signal 2 $pid
	if $nu.os-info.name == 'linux' {
		^tail --pid $pid -f /dev/null
	} else {
		while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
	}
}

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
	indexer: { database_outbox_wakeup_interval: 0.01 },
	regions: $regions,
}
let producer = $common | merge { roles: [cleaner http runner scheduler] }
let east = spawn --name east --directory $east_directory --url $east_url --config ($producer | merge { region: 'east' })
let west = spawn --name west --directory $west_directory --url $west_url --config ($producer | merge { region: 'west' })

let east_group = tg --url $east.url group create east-project | from json
let west_group = tg --url $west.url group create west-project | from json
let rows = (
	open $database_path
	| query db 'select region, batch from outbox order by batch, region'
)
assert equal ($rows | get batch) [1 1 2 2]
assert equal ($rows | get region) [east west east west]
let next = open $database_path | query db 'select next from outbox_batch' | get next.0
assert equal $next 2

stop $east
stop $west

let east = spawn --name east-indexer --directory $east_directory --url $east_url --config ($common | merge { region: 'east' })
let west = spawn --name west-indexer --directory $west_directory --url $west_url --config ($common | merge { region: 'west' })
tg --url $east.url index
tg --url $west.url index

let indexed = tg --url $west.url group get east-project | from json
assert equal $indexed.id $east_group.id
let indexed = tg --url $east.url group get west-project | from json
assert equal $indexed.id $west_group.id
