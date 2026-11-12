use ../../util/test.nu *

# The minimum retention windows preserve children until their parents can be aggregated.

def --wrapped usage [token: string, ...period: string] {
	tg --token $token usage ...$period | from json
}

def --wrapped unavailable [token: string, ...period: string] {
	let output = tg --token $token usage ...$period | complete
	failure $output "expired usage should be unavailable"
	assert ($output.stderr | str contains "usage is unavailable for the requested period")
}

let root_token = random chars
let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer runner scheduler],
	scheduler: { runner_ttl: 300 },
	usage: {
		day_ttl: 2678400,
		delta_ttl: 3600,
		hour_ttl: 86400,
		month_ttl: 2678400,
		week_ttl: 604800,
	},
}
let alice = tg login --verbose --name alice | from json
let object = tg --token $alice.token put --no-tokens 'tg.file("keep")' | referent node
tg --token $alice.token tag keep $object
tg --token $alice.token index --database

# The first hour remains available immediately before the day closes.
set_time $local '2026-01-01T23:59:59Z'
tg --token $alice.token clean
assert equal (usage $alice.token --hour 2026-01-01T00:00:00Z).object_count 2

# After the day is aggregated and the hourly ttl elapses, expiration can remove the first hour.
set_time $local '2026-01-02T01:00:00Z'
tg --token $alice.token clean
unavailable $alice.token --hour 2026-01-01T00:00:00Z
assert equal (usage $alice.token --day 2026-01-01).object_count 48
