use ../../test.nu *

# Remove and rename events for a watched single-file root invalidate an incremental checkin that already snapshotted it.

let server = spawn --config {
	advanced: {
		checkpoints: true
	}
}

def checkin_background [path: path] {
	job spawn {
		let job_id = job id
		let output = tg checkin $path --watch --no-cache-pointers --no-lock | complete
		$output | job send --tag $job_id 0
	}
}

def check_event [kind: string] {
	let path = artifact 'alpha'
	tg checkin $path --watch --no-cache-pointers --no-lock | ignore

	# Hold an incremental checkin after it snapshots the watched file.
	let snapshot_watch = (
		tg checkpoint watch checkin.watch.snapshot --params '{"solve":true,"updates":""}'
		| from json
		| get watch
	)
	let checkin = checkin_background $path
	tg checkpoint wait checkin.watch.snapshot $snapshot_watch 0 | ignore

	# Synchronously inject the event for the watched root.
	tg watch touch $path $path --kind $kind

	# The checkin must reject the stale snapshot.
	tg checkpoint continue checkin.watch.snapshot $snapshot_watch 0
	tg checkpoint unwatch checkin.watch.snapshot $snapshot_watch
	let output = job recv --tag $checkin --timeout 10sec
	failure $output $"the checkin should reject a concurrent ($kind) event"
}

for kind in [remove rename] {
	check_event $kind
}
