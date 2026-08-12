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

# A completed period before usage tracking started is unavailable.
let previous_hour = tg --token $alice.token usage --hour 2025-12-31T23:00:00Z | complete
failure $previous_hour "usage before tracking started should be unavailable"
assert ($previous_hour.stderr | str contains "usage is unavailable for the requested period")

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
let leap_day = usage $alice.token --day 2028-02-29
assert equal $leap_day.period.start '2028-02-29T00:00:00Z'
assert equal $leap_day.period.end '2028-03-01T00:00:00Z'

let iso_week = usage $alice.token --week 2026-W01
assert equal $iso_week.period.start '2025-12-29T00:00:00Z'
assert equal $iso_week.period.end '2026-01-05T00:00:00Z'

# Invalid, conflicting, and overflowing selectors fail without crashing the server.
failure (tg --token $alice.token usage --hour 2026-01-01T00:30:00Z | complete) "an unaligned hour should fail"
failure (tg --token $alice.token usage --day 2026-01-01 --month 2026-01 | complete) "conflicting periods should fail"
failure (tg --token $alice.token usage --day 2026-02-29 | complete) "an invalid date should fail"
failure (tg --token $alice.token usage --day 9999-12-31 | complete) "an overflowing date should fail"

let object = tg --token $alice.token put 'tg.file("hello")' | str trim
failure (tg --token $alice.token usage $object | complete) "a non-account ID should fail"

# The server remains usable after rejecting all invalid inputs.
success (tg --token $alice.token usage --hour 2026-01-01T00:00:00Z | complete)
