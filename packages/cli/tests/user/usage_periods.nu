use ../../test.nu *

# Usage period selectors use exact UTC boundaries and reject ambiguous or unrepresentable input.

def --wrapped usage [token: string, ...period: string] {
	tg --token $token usage ...$period | from json
}

let server = spawn --now '2026-01-01T00:00:00Z' --config {
	authentication: { users: { providers: { insecure: true } } },
	usage: true,
}
let alice = tg login --verbose alice | from json

# The default is the current UTC month, including at its exact start boundary.
let current_month = usage $alice.token
assert equal $current_month.period.kind month
assert equal $current_month.period.start '2026-01-01T00:00:00Z'
assert equal $current_month.period.end '2026-02-01T00:00:00Z'
assert not $current_month.complete

# A period ending exactly at now is complete; current and future periods are not.
let previous_hour = usage $alice.token --hour 2025-12-31T23:00:00Z
assert $previous_hour.complete
assert equal $previous_hour.period.end '2026-01-01T00:00:00Z'

let current_hour = usage $alice.token --hour 2026-01-01T00:00:00Z
assert not $current_hour.complete
assert equal $current_hour.period.start '2026-01-01T00:00:00Z'
assert equal $current_hour.period.end '2026-01-01T01:00:00Z'

let future_hour = usage $alice.token --hour 2026-01-01T01:00:00Z
assert not $future_hour.complete
assert equal $future_hour.sandbox_cpu 0
assert equal $future_hour.sandbox_memory 0
assert equal $future_hour.object_count 0
assert equal $future_hour.object_size 0
assert equal $future_hour.process_count 0
assert equal $future_hour.sandbox_count 0

# Calendar periods handle leap days and ISO weeks that cross calendar years.
let leap_day = usage $alice.token --day 2024-02-29
assert equal $leap_day.period.start '2024-02-29T00:00:00Z'
assert equal $leap_day.period.end '2024-03-01T00:00:00Z'

let iso_week = usage $alice.token --week 2020-W53
assert equal $iso_week.period.start '2020-12-28T00:00:00Z'
assert equal $iso_week.period.end '2021-01-04T00:00:00Z'

# Invalid, conflicting, and overflowing selectors fail without crashing the server.
failure (tg --token $alice.token usage --hour 2026-01-01T00:30:00Z | complete) "an unaligned hour should fail"
failure (tg --token $alice.token usage --day 2026-01-01 --month 2026-01 | complete) "conflicting periods should fail"
failure (tg --token $alice.token usage --day 2026-02-29 | complete) "an invalid date should fail"
failure (tg --token $alice.token usage --day 9999-12-31 | complete) "an overflowing date should fail"

let object = tg --token $alice.token put 'tg.file("hello")' | str trim
failure (tg --token $alice.token usage $object | complete) "a non-account ID should fail"

# The server remains usable after rejecting all invalid inputs.
success (tg --token $alice.token usage --hour 2026-01-01T00:00:00Z | complete)
