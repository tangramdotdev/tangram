use ../../util/test.nu *

# Capacity cleaning removes an unreferenced object before its time to live expires.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true },
	indexer: {
		cleaning: {
			capacity: {
				kind: bytes
				start_above: 1
				stop_at: 0
			}
		}
	}
	object: { ttl: 86400 }
}

# Watch every object deletion before the put, because capacity cleaning may collect the object as soon as it is indexed.
let delete = checkpoint watch cleaning.object.delete
let object = tg put --no-tokens 'tg.file("clean me")' | referent node
tg index

# Advance the clock to wake cleaning, and wait for it to delete the object.
advance_time $local 1min
mut hit = 0
while (checkpoint pass $delete $hit | get params.object) != $object {
	$hit += 1
}
checkpoint unwatch $delete
let output = tg object get $object | complete
failure $output "the object should be collected under capacity pressure"
