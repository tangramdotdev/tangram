use ../../test.nu *

# A watchdog only monitors schedulers in its own region.

def schedulers [database: path] {
	^sqlite3 -separator ':' $database 'select region, status from schedulers order by region;' | lines
}

let database_directory = mktemp -d
let database = $database_directory | path join 'database'
let east_directory = mktemp -d
let west_directory = mktemp -d
let east_url = $'http+unix://($east_directory | url encode --all)%2Fsocket'
let west_url = $'http+unix://($west_directory | url encode --all)%2Fsocket'
let regions = [
	{ name: 'east', url: $east_url },
	{ name: 'west', url: $west_url },
]
let common = {
	database: { kind: 'sqlite', path: $database },
	regions: $regions,
	watchdog: {
		interval: 0.05,
		ttl: 2,
	},
}
let east = spawn --name east --directory $east_directory --url $east_url --config ($common | merge { region: 'east' })

wait_until { (schedulers $database) == ['east:started'] } 'the east scheduler should start'

let west = spawn --name west --directory $west_directory --url $west_url --config ($common | merge { region: 'west' })

wait_until { (schedulers $database) == ['east:started', 'west:started'] } 'the regional schedulers should start'

# Stop the east server and wait beyond the watchdog TTL. The west watchdog must not stop the east scheduler.
let east_pid = open ($east.directory | path join 'lock') | into int
kill --signal 9 $east_pid
if $nu.os-info.name == 'linux' {
	^tail --pid $east_pid -f /dev/null
} else {
	while (ps | where pid == $east_pid | is-not-empty) { sleep 10ms }
}
sleep 3sec

assert equal (schedulers $database) ['east:started', 'west:started']
