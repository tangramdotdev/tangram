use ../../test.nu *

# A checkin replacing a watch with incompatible options rejects a concurrent filesystem change.

let server = spawn --config {
	advanced: {
		checkpoints: true
	}
}

let path = artifact {
	tangram.ts: 'export default "before";'
}
tg checkin $path --watch --no-cache-pointers --no-lock | ignore

def checkin_background [path: path] {
	job spawn {
		let job_id = job id
		let output = tg checkin $path --watch --no-cache-pointers --no-lock --no-solve | complete
		$output | job send --tag $job_id 0
	}
}

# Hold an incompatible checkin after it has read the filesystem but before it replaces the watch.
let publish_watch = (
	tg checkpoint watch checkin.watch.publish --params '{"solve":false,"updates":""}'
	| from json
	| get watch
)
let checkin = checkin_background $path
tg checkpoint wait checkin.watch.publish $publish_watch 0 | ignore

# Modify the input and synchronously deliver the event to the existing watch.
'export default "after";' | save --force ($path | path join tangram.ts)
tg watch touch $path ($path | path join tangram.ts)

# The incompatible checkin must not replace the changed watch with its stale graph.
tg checkpoint continue checkin.watch.publish $publish_watch 0
tg checkpoint unwatch checkin.watch.publish $publish_watch
let output = job recv --tag $checkin --timeout 10sec
failure $output "the incompatible checkin should reject a concurrent filesystem change"
