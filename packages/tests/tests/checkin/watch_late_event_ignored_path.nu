use ../../util/test.nu *

# A watched checkin succeeds when the watch applies the creation of an ignored path, which happened before the checkin started, during the checkin.

let local = server spawn --config {
	advanced: {
		checkpoints: true
	}
}

let path = artifact {
	".tangramignore": 'ignored'
	"a.txt": 'one'
	"b.txt": 'b'
}
tg checkin --no-tokens $path --watch | ignore

# Hold the watch after it applies another edit, so that it has not applied the creation below when the checkin starts.
let hold = checkpoint watch watch.event --params { path: ($path | path join b.txt) }
'changed' | save --force ($path | path join b.txt)
checkpoint wait $hold | ignore
mkdir ($path | path join ignored)

# Start a checkin, hold it after it reads the snapshot of the watch, and let the watch apply the creation.
let snapshot_watch = tg checkpoint watch checkin.watch.snapshot | from json | get watch
let checkin = job spawn {
	let job_id = job id
	let output = tg checkin --no-tokens $path --watch | complete
	$output | job send --tag $job_id 0
}
tg checkpoint wait checkin.watch.snapshot $snapshot_watch 0 | ignore
let event = checkpoint watch watch.event --params { path: ($path | path join ignored) }
checkpoint unwatch $hold
checkpoint wait $event | ignore
checkpoint unwatch $event
tg checkpoint continue checkin.watch.snapshot $snapshot_watch 0
tg checkpoint unwatch checkin.watch.snapshot $snapshot_watch

# The checkin must succeed, because nothing changed during the checkin.
let output = job recv --tag $checkin --timeout 10sec
success $output
let cold = tg checkin --no-watch --no-tokens $path | referent node
assert equal ($output.stdout | referent node) $cold
