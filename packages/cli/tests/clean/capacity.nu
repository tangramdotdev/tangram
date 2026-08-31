use ../../test.nu *

# Capacity cleaning removes an unreferenced object before its time to live expires.

let server = server spawn --config {
	indexer: {
		cleaning: {
			capacity: {
				kind: bytes
				start_above: 1
				stop_at: 0
			}
			poll_interval: 0.1
		}
	}
	object: { ttl: 86400 }
}

let object = tg put 'tg.file("clean me")' | str trim
tg index

wait_until {
	(tg object get $object | complete).exit_code != 0
} "the object should be collected under capacity pressure"
