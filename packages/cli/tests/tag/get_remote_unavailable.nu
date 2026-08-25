use ../../test.nu *

# Getting a tag that only exists on the remote fails promptly when the remote server is not running.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Create and tag an object on the remote.
let id = tg --url $remote.url put 'tg.file("test")' | str trim
tg --url $remote.url tag -p remote-only/1.0.0 $id

# Kill the remote server.
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the remote should stop"

let output = tg tag get remote-only/1.0.0 | complete
failure $output
