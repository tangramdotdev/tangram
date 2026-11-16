use ../../util/test.nu *

# A file changed after the first watched checkin reads it and before the watch registers its paths invalidates the file.

let local = server spawn --config {
	advanced: {
		checkpoints: true
	}
}

let path = artifact {
	directory: {
		"a.txt": 'a'
	}
}
let directory = $path | path join directory

# Hold the first watched checkin after it reads the files, and change a file.
let publish_watch = tg checkpoint watch checkin.watch.publish | from json | get watch
let checkin = job spawn {
	let job_id = job id
	let output = tg checkin --no-tokens $path --watch | complete
	$output | job send --tag $job_id 0
}
tg checkpoint wait checkin.watch.publish $publish_watch 0 | ignore
'changed' | save --force ($directory | path join a.txt)
tg checkpoint continue checkin.watch.publish $publish_watch 0
tg checkpoint unwatch checkin.watch.publish $publish_watch
job recv --tag $checkin --timeout 10sec | ignore

# The next watched checkin must return the id of a cold checkin.
let cold = tg checkin --no-watch --no-tokens $path | referent node
let watched = tg checkin --no-tokens $path --watch | referent node
assert ($watched == $cold) "the watched checkin should equal a cold checkin"
