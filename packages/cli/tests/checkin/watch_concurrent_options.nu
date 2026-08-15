use ../../test.nu *

# A checkin cannot update a watcher that was replaced by a concurrent checkin with different options.

let server = spawn --config {
	advanced: {
		checkpoints: true
	}
}

let dependency_path = artifact { tangram.ts: '// a 1.0.0' }
tg tag -p a/1.0.0 $dependency_path

let path = artifact {
	tangram.ts: 'import a from "a/*";'
}
tg checkin $path --watch --no-checkout-pointers --no-lock | ignore

def checkin_background [path: path] {
	job spawn {
		let job_id = job id
		let output = tg checkin $path --watch --no-checkout-pointers --no-lock | complete
		$output | job send --tag $job_id 0
	}
}

# Hold a solved checkin after it snapshots the solved watcher.
let snapshot_watch = (
	tg checkpoint watch checkin.watch.snapshot --params '{"solve":true,"updates":""}'
	| from json
	| get watch
)
let solved_checkin = checkin_background $path
tg checkpoint wait checkin.watch.snapshot $snapshot_watch 0 | ignore

# Replace the watcher with no-solve state, then allow the stale solved checkin to finish.
let unsolved = tg checkin $path --watch --no-checkout-pointers --no-lock --no-solve
tg checkpoint continue checkin.watch.snapshot $snapshot_watch 0
tg checkpoint unwatch checkin.watch.snapshot $snapshot_watch
let solved_output = job recv --tag $solved_checkin --timeout 10sec
failure $solved_output "the stale solved checkin should not update the replacement watcher"

# The replacement watcher must retain no-solve state.
let watched = tg checkin $path --watch --no-checkout-pointers --no-lock --no-solve
let cold = tg checkin $path --no-checkout-pointers --no-lock --no-solve
assert ($unsolved == $cold) "the concurrent no-solve checkin should match a cold checkin"
assert ($watched == $cold) "the no-solve watcher should not contain stale solved state"
