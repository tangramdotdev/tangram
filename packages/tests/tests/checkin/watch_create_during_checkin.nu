use ../../util/test.nu *

# A file created in an invalidated directory while a watched checkin reads it again invalidates the directory.

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
tg checkin --no-tokens $path --watch | ignore

# Invalidate the directory.
tg watch touch $path $directory

# Hold a watched checkin after it reads the directory again, and create a file in the directory.
let publish_watch = tg checkpoint watch checkin.watch.publish | from json | get watch
let checkin = job spawn {
	let job_id = job id
	let output = tg checkin --no-tokens $path --watch | complete
	$output | job send --tag $job_id 0
}
tg checkpoint wait checkin.watch.publish $publish_watch 0 | ignore
'b' | save ($directory | path join b.txt)
tg checkpoint continue checkin.watch.publish $publish_watch 0
tg checkpoint unwatch checkin.watch.publish $publish_watch
job recv --tag $checkin --timeout 10sec | ignore

# The watched checkin must settle on the id of a cold checkin.
let cold = tg checkin --no-watch --no-tokens $path | referent node
wait_until --timeout 10sec {
	(tg checkin --no-tokens $path --watch | referent node) == $cold
} "the watched checkin should equal a cold checkin"
