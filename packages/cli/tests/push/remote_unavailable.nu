use ../../test.nu *

# Pushing fails promptly when the remote server is not running.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let id = tg put 'tg.file("test")' | str trim

# Kill the remote server.
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the remote should stop"

let output = tg push $id | complete
failure $output
snapshot ($output.stderr | redact $id $remote.url $remote.directory) '
	error an error occurred
	-> failed to create the pull stream
	-> failed to sync
	   remote = default
	-> failed to send the request
	-> failed to resolve the socket path
	   path = <path>/socket
	-> No such file or directory (os error 2)

'
