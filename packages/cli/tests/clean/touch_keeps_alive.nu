use ../../test.nu *

# The background cleaner collects an untouched object after its time to live while a continuously touched object survives.

let server = spawn --config { cleaner: {}, object: { ttl: 2, ttt: 0 } }

let touched = tg put 'tg.file("keep me alive")' | str trim
let untouched = tg put 'tg.file("let me die")' | str trim
tg index

# Touch one object while waiting for the cleaner to collect the other.
wait_until {
	tg touch $touched
	(tg object get $untouched | complete).exit_code != 0
} "the untouched object should be collected"

let output = tg object get $touched | complete
success $output
