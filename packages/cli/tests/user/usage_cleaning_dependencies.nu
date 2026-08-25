use ../../test.nu *

# The minimum retention windows preserve children until their parents can be aggregated.

def --wrapped usage [token: string, ...period: string] {
	tg --token $token usage ...$period | from json
}

def --wrapped unavailable [token: string, ...period: string] {
	let output = tg --token $token usage ...$period | complete
	failure $output "cleaned usage should be unavailable"
	assert ($output.stderr | str contains "usage is unavailable for the requested period")
}

let server = server spawn --now '2026-01-01T00:00:00Z' --config {
	authentication: { users: { providers: { insecure: true } } },
	roles: [http indexer runner scheduler],
	usage: {
		day_time_to_live: 2678400,
		delta_time_to_live: 3600,
		hour_time_to_live: 86400,
		month_time_to_live: 2678400,
		week_time_to_live: 604800,
	},
}
let alice = tg login --verbose --name alice | from json
let object = tg --token $alice.token put 'tg.file("keep")' | str trim
tg --token $alice.token tag keep $object
tg --token $alice.token index

# The first hour remains available immediately before the day closes.
set_time $server '2026-01-01T23:59:59Z'
tg --token $alice.token clean
assert equal (usage $alice.token --hour 2026-01-01T00:00:00Z).object_count 2

# After the day is aggregated and the hourly TTL elapses, cleaning can expire the first hour.
set_time $server '2026-01-02T01:00:00Z'
tg --token $alice.token clean
unavailable $alice.token --hour 2026-01-01T00:00:00Z
assert equal (usage $alice.token --day 2026-01-01).object_count 48
