use ../../test.nu *

# Pulling fails promptly when the remote server is not running.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Create and tag an object on the remote.
let id = tg --url $remote.url put 'tg.file("test")' | str trim
tg --url $remote.url tag test/1.0.0 $id

# Kill the remote server.
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the remote should stop"

let output = tg pull $id | complete
failure $output
assert ($output.stderr | str contains 'failed to sync') "the error should mention the failed sync"
