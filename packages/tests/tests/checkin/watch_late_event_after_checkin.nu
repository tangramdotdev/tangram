use ../../util/test.nu *

# A watched checkin returns the contents of a file that was edited before the checkin started when the watch has not yet applied the edit.

let local = server spawn --config {
	advanced: {
		checkpoints: true
	}
}

let path = artifact {
	"a.txt": 'one'
	"b.txt": 'b'
}
tg checkin --no-tokens $path --watch | ignore

# Hold the watch after it applies another edit, so that it does not apply the edit below.
let hold = checkpoint watch watch.event --params { path: ($path | path join b.txt) }
'changed' | save --force ($path | path join b.txt)
checkpoint wait $hold | ignore
'two' | save --force ($path | path join a.txt)

# The watched checkin must return the id of a cold checkin.
let cold = tg checkin --no-watch --no-tokens $path | referent node
let watched = tg checkin --no-tokens $path --watch | referent node
checkpoint unwatch $hold
assert ($watched == $cold) "the watched checkin should equal a cold checkin"
