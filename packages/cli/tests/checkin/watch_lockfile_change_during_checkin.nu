use ../../test.nu *

# A watched checkin rejects a lockfile change made after it snapshots the watch.

let server = spawn --config {
	advanced: {
		checkpoints: true
	}
}

let a_1_0_path = artifact {
	tangram.ts: '// a 1.0.0'
}
tg tag -p a/1.0.0 $a_1_0_path

let a_1_0_id = tg tag get a/1.0.0 | from json | get target.id
let lock = {
	nodes: [
		{
			kind: directory
			entries: {
				tangram.ts: {
					index: 1
					kind: file
				}
			}
		}
		{
			kind: file
			dependencies: {
				'a/^1': {
					item: null
					options: {
						id: $a_1_0_id
						tag: a/1.0.0
					}
				}
			}
			module: ts
		}
	]
}
let path = artifact {
	tangram.ts: 'import a from "a/^1";'
	tangram.lock: ($lock | to json)
}

# Establish a watch with the original lock.
tg checkin $path --watch --locked | ignore

def checkin_background [path: path] {
	job spawn {
		let job_id = job id
		let output = tg checkin $path --watch --locked | complete
		$output | job send --tag $job_id 0
	}
}

# Hold the next checkin after it snapshots the watch.
let snapshot_watch = (
	tg checkpoint watch checkin.watch.snapshot --params '{"solve":true,"updates":""}'
	| from json
	| get watch
)
let checkin = checkin_background $path
tg checkpoint wait checkin.watch.snapshot $snapshot_watch 0 | ignore

# Change the lockfile and synchronously deliver its watch event.
let lockfile_path = $path | path join tangram.lock
{ nodes: [] } | to json | save --force $lockfile_path
tg watch touch $path $lockfile_path

# The checkin must reject the stale snapshot.
tg checkpoint continue checkin.watch.snapshot $snapshot_watch 0
tg checkpoint unwatch checkin.watch.snapshot $snapshot_watch
let output = job recv --tag $checkin --timeout 10sec
failure $output "the checkin should reject a concurrent lockfile change"
