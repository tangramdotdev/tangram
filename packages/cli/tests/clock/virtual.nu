use ../lib/test.nu *

# A server's virtual clock reports its time, moves only when set or advanced, and can be set to an earlier time.

let local = server spawn --now '2026-01-01T00:00:00Z'
assert equal (tg clock get | from json | get now) '2026-01-01T00:00:00Z'

# Advancing the clock moves its time by the duration.
assert equal (tg clock advance 90s | from json | get now) '2026-01-01T00:01:30Z'
assert equal (tg clock get | from json | get now) '2026-01-01T00:01:30Z'

# Setting the clock moves its time to the timestamp, whether it is later or earlier.
assert equal (tg clock set '2026-01-02T00:00:00Z' | from json | get now) '2026-01-02T00:00:00Z'
assert equal (tg clock set '2025-12-31T00:00:00Z' | from json | get now) '2025-12-31T00:00:00Z'
assert equal (tg clock get | from json | get now) '2025-12-31T00:00:00Z'

# A restarted server resumes from the time that the test library recorded.
advance_time $local 1hr
let local = server restart $local
assert equal (tg clock get | from json | get now) '2025-12-31T01:00:00Z'

# A server without a virtual clock rejects clock requests.
let other = server spawn --name other
failure (tg --url $other.url clock get | complete) 'a server without a virtual clock must reject clock requests'
