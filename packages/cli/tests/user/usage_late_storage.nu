use ../../test.nu *

# Late storage deltas re-aggregate every period they affect.

def --wrapped usage [token: string, ...period: string] {
	tg --token $token usage ...$period | from json
}

let server = spawn --now '2025-12-29T00:00:00Z' --config {
	authentication: { users: { providers: { insecure: true } } },
	roles: [http indexer runner scheduler],
	usage: true,
}
set_time $server '2026-01-01T00:00:00Z'
let alice = tg login --verbose alice | from json

let first = tg --token $alice.token put 'tg.file("first")' | str trim
tg --token $alice.token tag first $first
tg --token $alice.token index

# Aggregate every completed period containing the first storage event.
set_time $server '2026-02-02T00:00:00Z'
let hour_before = usage $alice.token --hour 2026-01-01T01:00:00Z
let day_before = usage $alice.token --day 2026-01-01
let week_before = usage $alice.token --week 2026-W01
let month_before = usage $alice.token --month 2026-01
assert equal $hour_before.object_count 2
assert equal $day_before.object_count 48
assert equal $week_before.object_count 192
assert equal $month_before.object_count 1488

# Index another object in the original hour after the aggregates already exist.
set_time $server '2026-01-01T00:30:00Z'
let second = tg --token $alice.token put 'tg.file("second")' | str trim
tg --token $alice.token tag second $second
tg --token $alice.token index

# Every later storage gauge must include the late event after re-aggregation.
set_time $server '2026-02-02T00:00:00Z'
assert equal (usage $alice.token --hour 2026-01-01T01:00:00Z).object_count 4
assert equal (usage $alice.token --day 2026-01-01).object_count 96
assert equal (usage $alice.token --week 2026-W01).object_count 384
assert equal (usage $alice.token --month 2026-01).object_count 2976
