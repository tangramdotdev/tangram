use ../../test.nu *

# Concurrent selective updates compare against the watcher revision they originally observed.

let server = spawn --config {
	advanced: {
		checkpoints: true
	}
}

for name in [a b] {
	let dependency_path = artifact { tangram.ts: $'// ($name) 1.0.0' }
	tg tag -p $'($name)/1.0.0' $dependency_path
}

let path = artifact {
	tangram.ts: '
		import a from "a/*";
		import b from "b/*";
	'
}
tg checkin $path --watch --no-cache-pointers --no-lock | ignore

def checkin_background [path: path] {
	job spawn {
		let job_id = job id
		let output = tg checkin $path --watch --no-cache-pointers --no-lock --update a | complete
		$output | job send --tag $job_id 0
	}
}

# Hold update a after it snapshots the watcher, then let update b commit first.
let snapshot_watch = (
	tg checkpoint watch checkin.watch.snapshot --params '{"updates":"a"}'
	| from json
	| get watch
)
let update_a = checkin_background $path
tg checkpoint wait checkin.watch.snapshot $snapshot_watch 0 | ignore
tg checkin $path --watch --no-cache-pointers --no-lock --update b | ignore

# The stale update must fail rather than overwrite the newer watcher revision.
tg checkpoint continue checkin.watch.snapshot $snapshot_watch 0
tg checkpoint unwatch checkin.watch.snapshot $snapshot_watch
let output = job recv --tag $update_a --timeout 10sec
failure $output "the stale selective update should not overwrite the watcher"
