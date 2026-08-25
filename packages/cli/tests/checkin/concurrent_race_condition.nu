use ../../test.nu *

# A checkin that subscribes after the shared root task finishes still receives its
# output. Regression test for 4819305a (#734).

let server = server spawn --config {
	advanced: {
		checkpoints: true,
	},
}

let path = artifact {
	tangram.ts: 'export default 0;'
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
	if $output == null {
		error make { msg: 'the checkin timed out' }
	}
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

# Let the first checkin subscribe, but hold the shared task before it outputs.
let first = checkin_background $path
let subscribe_hit = tg checkpoint wait checkin.progress.subscribe $subscribe_watch 0 | from json
let subscribed_watch = (
	tg checkpoint watch checkin.progress.subscribed --params ($subscribe_hit.params | to json --raw)
	| from json
	| get watch
)
tg checkpoint continue checkin.progress.subscribe $subscribe_watch 0
tg checkpoint wait checkin.progress.subscribed $subscribed_watch 0 | ignore
tg checkpoint continue checkin.progress.subscribed $subscribed_watch 0
tg checkpoint unwatch checkin.progress.subscribed $subscribed_watch
tg checkpoint wait checkin.progress.output $output_watch 0 | ignore

# Hold the second checkin before it subscribes, then let the shared task output.
let second = checkin_background $path
tg checkpoint wait checkin.progress.subscribe $subscribe_watch 1 | ignore
tg checkpoint continue checkin.progress.output $output_watch 0
receive_checkin $first

# The second checkin must receive the terminal event when it subscribes late.
tg checkpoint continue checkin.progress.subscribe $subscribe_watch 1
receive_checkin $second

tg checkpoint unwatch checkin.progress.output $output_watch
tg checkpoint unwatch checkin.progress.subscribe $subscribe_watch
