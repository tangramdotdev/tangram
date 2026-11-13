use ../../util/test.nu *

# A watched checkin matches a cold checkin after the kernel drops file events because its inotify queue overflowed.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

# Give the watch a channel that holds every event the kernel queues, so that only the kernel drops events.
let count = open /proc/sys/fs/inotify/max_queued_events | into int
let local = server spawn --config {
	watch: {
		channel_capacity: ($count * 2)
	}
}

let path = artifact {
	"a.txt": ''
	"b.txt": 'one'
}
tg checkin --no-tokens $path --watch | ignore

# Pause the server, queue more file events than the kernel holds, and edit a file whose events the kernel drops.
kill --signal 19 $local.pid
for i in 0..<$count {
	'' | save --append ($path | path join a.txt)
}
'two' | save --force ($path | path join b.txt)
kill --signal 18 $local.pid

# The watched checkin must settle on the id of a cold checkin.
let cold = tg checkin --no-watch --no-tokens $path | referent node
wait_until --timeout 30sec {
	(tg checkin --no-tokens $path --watch | referent node) == $cold
} "the watched checkin should equal a cold checkin"
