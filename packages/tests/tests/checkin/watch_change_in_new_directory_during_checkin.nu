use ../../util/test.nu *

# A file created in a new directory after a watched checkin reads the directory and before the watch registers it invalidates the directory.

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
tg checkin --no-tokens $path --watch | ignore

# Create a new directory, and invalidate its parent so that the next watched checkin reads it.
let directory = $path | path join directory new
mkdir $directory
'b' | save ($directory | path join b.txt)
tg watch touch $path ($path | path join directory)

# Hold a watched checkin after it reads the new directory, and create a file in it.
let publish_watch = tg checkpoint watch checkin.watch.publish | from json | get watch
let checkin = job spawn {
	let job_id = job id
	let output = tg checkin --no-tokens $path --watch | complete
	$output | job send --tag $job_id 0
}
tg checkpoint wait checkin.watch.publish $publish_watch 0 | ignore
'c' | save ($directory | path join c.txt)
tg checkpoint continue checkin.watch.publish $publish_watch 0
tg checkpoint unwatch checkin.watch.publish $publish_watch
job recv --tag $checkin --timeout 10sec | ignore

# The watched checkin must settle on the id of a cold checkin.
let cold = tg checkin --no-watch --no-tokens $path | referent node
wait_until --timeout 10sec {
	(tg checkin --no-tokens $path --watch | referent node) == $cold
} "the watched checkin should equal a cold checkin"
