use ../../util/test.nu *

# Printing an error from a failing `server stop` does not spawn a server, even when start is enabled.

# Spawn a process that holds the lock on a lock file that names it with systemd as its manager, as a server that systemd started does, so that the stop is refused.
let directory = mktemp -d
let ready = mktemp --directory | path join 'ready'
let release = mktemp --directory | path join 'release'
^mkfifo $ready $release
let script = '
import fcntl, json, os, sys
with open(sys.argv[1], "w") as lock:
	fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
	lock.write(json.dumps({"manager": "systemd", "pid": os.getpid()}))
	lock.flush()
	with open(sys.argv[2], "w") as ready:
		ready.write("ready")
	with open(sys.argv[3]) as release:
		release.read()
'
let holder = job spawn {
	^python3 -c $script ($directory | path join 'lock') $ready $release | complete | job send 0
}
open --raw $ready | ignore

let output = tg --start -d $directory server stop | complete
failure $output

# Record whether a server appeared, then stop it before asserting, so that a failure of this test does not leak a server the harness cannot reap.
let spawned = ($directory | path join 'socket') | path exists
if $spawned {
	tg -d $directory server stop | complete | ignore
}

# Release the process that holds the lock.
'' | save --append $release
let output = job recv --timeout 10sec
success $output 'the process that holds the lock should exit after it is released'

assert not $spawned 'printing the error should not have spawned a server'
