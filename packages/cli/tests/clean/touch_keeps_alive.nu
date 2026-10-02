use ../lib/test.nu *

# Background cleaning collects an untouched object after its time to live while a continuously touched object survives.

let local = server spawn --config { indexer: { cleaning: {} }, object: { ttl: 2, ttt: 0 } }

let touched = tg put --no-tokens 'tg.file("keep me alive")' | referent node
let untouched = tg put --no-tokens 'tg.file("let me die")' | referent node
tg index

# Touch one object while waiting for cleaning to collect the other.
wait_until {
	tg touch $touched
	(tg object get $untouched | complete).exit_code != 0
} "the untouched object should be collected"

let output = tg object get $touched | complete
success $output
