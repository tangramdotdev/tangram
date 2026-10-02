use ../lib/test.nu *

# Pushing fails promptly when the remote server is not running.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let id = tg put --no-tokens 'tg.file("test")' | referent node

# Kill the remote server.
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the remote should stop"

let output = tg push $id | complete
failure $output
snapshot --normalize --redact [$id $remote.url $remote.directory ($remote.directory | path expand)] $output.stderr '
	error an error occurred
	-> failed to push
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to start the push
	-> failed to create the destination stream
	-> failed to sync
	   remote = default
	-> failed to send the request
	-> failed to resolve the socket path
	   path = <redacted>/socket
	-> No such file or directory (os error 2)

'
