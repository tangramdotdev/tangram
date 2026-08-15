use ../../test.nu *

# Extra remove and rename notifications are accepted, but real changes invalidate a snapshotted checkin.

let server = spawn --config {
	advanced: {
		checkpoints: true
	}
}

def checkin_background [path: path] {
	job spawn {
		let job_id = job id
		let output = tg checkin $path --watch --no-checkout-pointers --no-lock | complete
		$output | job send --tag $job_id 0
	}
}

def check_event [kind: string] {
	let path = artifact 'alpha'
	tg checkin $path --watch --no-checkout-pointers --no-lock | ignore

	# Hold an incremental checkin after it snapshots the watched file.
	let snapshot_watch = (
		tg checkpoint watch checkin.watch.snapshot --params '{"solve":true,"updates":""}'
		| from json
		| get watch
	)
	let checkin = checkin_background $path
	tg checkpoint wait checkin.watch.snapshot $snapshot_watch 0 | ignore

	# Deliver an extra notification without changing the watched root.
	tg watch touch $path $path --kind $kind

	# The checkin must revalidate and publish its graph.
	tg checkpoint continue checkin.watch.snapshot $snapshot_watch 0
	tg checkpoint unwatch checkin.watch.snapshot $snapshot_watch
	let output = job recv --tag $checkin --timeout 10sec
	success $output

	# Hold an incremental checkin after it snapshots the watched file.
	let snapshot_watch = (
		tg checkpoint watch checkin.watch.snapshot --params '{"solve":true,"updates":""}'
		| from json
		| get watch
	)
	let checkin = checkin_background $path
	tg checkpoint wait checkin.watch.snapshot $snapshot_watch 0 | ignore

	# Change the watched root and synchronously deliver the event.
	match $kind {
		remove => {
			rm $path
			tg watch touch $path $path --kind remove
		}
		rename => {
			let renamed_path = $path | path dirname | path join renamed
			mv $path $renamed_path
			tg watch touch $path $path $renamed_path --kind rename
		}
	}

	# The checkin must reject the stale snapshot.
	tg checkpoint continue checkin.watch.snapshot $snapshot_watch 0
	tg checkpoint unwatch checkin.watch.snapshot $snapshot_watch
	let output = job recv --tag $checkin --timeout 10sec
	failure $output $"the checkin should reject a concurrent ($kind) event"
}

for kind in [remove rename] {
	check_event $kind
}
