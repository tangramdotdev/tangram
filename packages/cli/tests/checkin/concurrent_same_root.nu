use ../../test.nu *

# Two checkins subscribed to the same root progress stream both receive its
# output.
#
# Regression test for 4245d307.

let server = server spawn --config {
	advanced: {
		checkpoints: true,
	},
}

let path = artifact {
	tangram.ts: 'export default "root";'
	a.txt: 'file a'
	b.txt: 'file b'
}

def checkin_background [path: path] {
	job spawn {
		let job_id = job id
		let output = tg checkin $path | complete
		$output | job send --tag $job_id 0
	}
}

def receive_checkin [job: int] {
	let output = job recv --tag $job --timeout 10sec
	success $output
}

let output_watch = (
	tg checkpoint watch checkin.progress.output
	| from json
	| get watch
)
let subscribe_watch = (
	tg checkpoint watch checkin.progress.subscribe
	| from json
	| get watch
)
let subscribed_watch = (
	tg checkpoint watch checkin.progress.subscribed
	| from json
	| get watch
)

let checkins = [a.txt b.txt] | each { |file|
	checkin_background ($path | path join $file)
}

# Create both subscriptions while holding the shared root task before output.
for hit in 0..1 {
	tg checkpoint wait checkin.progress.subscribe $subscribe_watch $hit | ignore
	tg checkpoint continue checkin.progress.subscribe $subscribe_watch $hit
	tg checkpoint wait checkin.progress.subscribed $subscribed_watch $hit | ignore
}
tg checkpoint wait checkin.progress.output $output_watch 0 | ignore

# Both receivers exist before the single terminal output is broadcast.
for hit in 0..1 {
	tg checkpoint continue checkin.progress.subscribed $subscribed_watch $hit
}
tg checkpoint continue checkin.progress.output $output_watch 0
tg checkpoint unwatch checkin.progress.output $output_watch
tg checkpoint unwatch checkin.progress.subscribe $subscribe_watch
tg checkpoint unwatch checkin.progress.subscribed $subscribed_watch

for checkin in $checkins {
	receive_checkin $checkin
}
