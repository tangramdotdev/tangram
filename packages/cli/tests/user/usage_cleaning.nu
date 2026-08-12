use ../../test.nu *

# Usage cleaning preserves coarser aggregates, expires each period independently, and retains zero storage checkpoints.

def --wrapped usage [token: string, ...period: string] {
	tg --token $token usage ...$period | from json
}

let server = spawn --now '2026-01-01T00:15:00Z' --config {
	authentication: { users: { providers: { insecure: true } } },
	roles: [http indexer runner scheduler],
	usage: {
		day_time_to_live: 2678400,
		delta_time_to_live: 3600,
		hour_time_to_live: 86400,
		month_time_to_live: 5184000,
		week_time_to_live: 3456000,
	},
}
let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

# Alice owns one retained object and one transient object. Bob owns one transient object.
let kept = tg --token $alice.token put 'tg.file("keep")' | str trim
tg --token $alice.token tag keep $kept
tg --token $alice.token put 'tg.file("remove")'
tg --token $bob.token put 'tg.file("remove")'
tg --token $alice.token index

# Cleaning in the next hour preserves the completed hour and records the new storage gauges.
advance_time $server 1hr
tg --token $alice.token clean
let alice_hour_0 = usage $alice.token --hour 2026-01-01T00:00:00Z
let alice_hour_1 = usage $alice.token --hour 2026-01-01T01:00:00Z
let bob_hour_0 = usage $bob.token --hour 2026-01-01T00:00:00Z
let bob_hour_1 = usage $bob.token --hour 2026-01-01T01:00:00Z
assert equal $alice_hour_0.object_count 4
assert equal $alice_hour_1.object_count 2
assert equal $bob_hour_0.object_count 2
assert equal $bob_hour_1.object_count 0

# Repeated cleaning must not subtract storage twice.
tg --token $alice.token clean
assert equal (usage $alice.token --hour 2026-01-01T01:00:00Z) $alice_hour_1
assert equal (usage $bob.token --hour 2026-01-01T01:00:00Z) $bob_hour_1

# An hourly aggregate expires at the exact TTL boundary, while the next hour remains available.
set_time $server '2026-01-02T01:00:00Z'
tg --token $alice.token clean
assert equal (usage $alice.token --hour 2026-01-01T00:00:00Z).object_count 0
assert equal (usage $bob.token --hour 2026-01-01T00:00:00Z).object_count 0
assert equal (usage $alice.token --hour 2026-01-01T01:00:00Z).object_count 2
assert equal (usage $bob.token --hour 2026-01-01T01:00:00Z).object_count 0

# The next boundary expires the following hour and retains the zero storage checkpoint.
advance_time $server 1hr
tg --token $alice.token clean
assert equal (usage $alice.token --hour 2026-01-01T01:00:00Z).object_count 0
assert equal (usage $bob.token --hour 2026-01-01T01:00:00Z).object_count 0
assert equal (usage $alice.token --day 2026-01-01).object_count 50
assert equal (usage $bob.token --day 2026-01-01).object_count 2
assert equal (usage $bob.token --hour 2026-01-02T02:00:00Z).object_count 0

# Daily aggregates expire after preserving the containing week and month.
set_time $server '2026-02-02T02:15:00Z'
tg --token $alice.token clean
assert equal (usage $alice.token --day 2026-01-01).object_count 0
assert equal (usage $bob.token --day 2026-01-01).object_count 0
assert equal (usage $alice.token --week 2026-W01).object_count 194
assert equal (usage $bob.token --week 2026-W01).object_count 2
assert equal (usage $alice.token --month 2026-01).object_count 1490
assert equal (usage $bob.token --month 2026-01).object_count 2

# Weekly and monthly aggregates expire according to their own retention periods.
set_time $server '2026-02-20T02:15:00Z'
tg --token $alice.token clean
assert equal (usage $alice.token --week 2026-W01).object_count 0
assert equal (usage $bob.token --week 2026-W01).object_count 0
assert equal (usage $alice.token --month 2026-01).object_count 1490
assert equal (usage $bob.token --month 2026-01).object_count 2

set_time $server '2026-04-03T02:15:00Z'
tg --token $alice.token clean
assert equal (usage $alice.token --month 2026-01).object_count 0
assert equal (usage $bob.token --month 2026-01).object_count 0
