use ../../test.nu *

# A push that fails because the remote is down does not corrupt state: an earlier push remains intact after the remote restarts, and a retry of the failed push succeeds.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Push an object.
let a = tg put 'tg.file("a")' | str trim
tg push $a
wait_until { (tg --url $remote.url get $a --local | complete).exit_code == 0 } "the first object should be present on the remote"

# Stop the remote server.
server stop $remote

# Push another object while the remote is down.
let b = tg put 'tg.file("b")' | str trim
let output = tg push $b | complete
failure $output

# Restart the remote server.
let remote = server start $remote

# The first object is still present on the remote.
let output = tg --url $remote.url get $a --local | complete
success $output

# Retrying the failed push succeeds.
let output = tg --url $local.url push $b | complete
success $output
