use ../lib/test.nu *

# Capacity cleaning removes an unreferenced object before its time to live expires.

let local = server spawn --config {
	advanced: { checkpoints: true },
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

# Watch every object deletion before the put, because capacity cleaning may collect the object as soon as it is indexed.
let delete = checkpoint watch cleaning.object.delete
let object = tg put --no-tokens 'tg.file("clean me")' | referent node
tg index

# Wait for cleaning to delete the object.
mut hit = 0
while (checkpoint pass $delete $hit | get params.object) != $object {
	$hit += 1
}
checkpoint unwatch $delete
let output = tg object get $object | complete
failure $output "the object should be collected under capacity pressure"
