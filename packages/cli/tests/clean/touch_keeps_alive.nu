use ../lib/test.nu *

# Background cleaning collects an untouched object after its time to live while a touched object survives.

let local = server spawn --now 2026-01-01T00:00:00Z --config { indexer: { cleaning: {} }, object: { ttl: 2, ttt: 0 } }

let touched = tg put --no-tokens 'tg.file("keep me alive")' | referent node
let untouched = tg put --no-tokens 'tg.file("let me die")' | referent node
tg index

# Touch one object a second later, so that only the other object's time to live expires when the clock reaches two seconds.
advance_time $local 1sec
tg touch $touched
tg index
advance_time $local 1sec

# Wait for cleaning to collect the untouched object.
wait_until {
	(tg object get $untouched | complete).exit_code != 0
} "the untouched object should be collected"

let output = tg object get $touched | complete
success $output
