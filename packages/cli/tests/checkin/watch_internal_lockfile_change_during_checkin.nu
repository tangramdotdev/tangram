use ../../test.nu *

# A watched checkin ignores the lockfile event caused by its own lock write.

let server = spawn --config {
	advanced: {
		checkpoints: true
	}
}

let dependency_path = artifact {
	tangram.ts: '// a 1.0.0'
}
tg tag -p a/1.0.0 $dependency_path

let path = artifact {
	tangram.ts: 'import a from "a/^1";'
}
tg checkin $path --watch | ignore

let dependency_path = artifact {
	tangram.ts: '// a 1.1.0'
}
tg tag -p a/1.1.0 $dependency_path

def update_background [path: path] {
	job spawn {
		let job_id = job id
		let output = tg checkin $path --watch --update a | complete
		$output | job send --tag $job_id 0
	}
}

# Hold the update after it writes the new lock but before it publishes the watch.
let publish_watch = (
	tg checkpoint watch checkin.watch.publish --params '{"solve":true,"updates":"a"}'
	| from json
	| get watch
)
let update = update_background $path
tg checkpoint wait checkin.watch.publish $publish_watch 0 | ignore

# Synchronously deliver the event caused by the internal lock write.
let lockfile_path = $path | path join tangram.lock
tg watch touch $path $lockfile_path

# The update must still be able to publish the watch.
tg checkpoint continue checkin.watch.publish $publish_watch 0
tg checkpoint unwatch checkin.watch.publish $publish_watch
let output = job recv --tag $update --timeout 10sec
success $output

let dependency_path = artifact {
	tangram.ts: '// a 1.2.0'
}
tg tag -p a/1.2.0 $dependency_path

# Hold another update with its internal lock write pending.
let publish_watch = (
	tg checkpoint watch checkin.watch.publish --params '{"solve":true,"updates":"a"}'
	| from json
	| get watch
)
let update = update_background $path
tg checkpoint wait checkin.watch.publish $publish_watch 0 | ignore

# An external write during that window must not be mistaken for the internal write.
{ nodes: [] } | to json | save --force $lockfile_path
tg watch touch $path $lockfile_path

tg checkpoint continue checkin.watch.publish $publish_watch 0
tg checkpoint unwatch checkin.watch.publish $publish_watch
let output = job recv --tag $update --timeout 10sec
failure $output "the update should reject an external lockfile write"
