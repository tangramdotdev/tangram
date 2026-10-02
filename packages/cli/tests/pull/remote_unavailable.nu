use ../lib/test.nu *

# Pulling fails promptly when the remote server is not running.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}
# Create and tag an object on the remote.
let id = tg --url $remote.url put --no-tokens 'tg.file("test")' | referent node
tg --url $remote.url tag -p test/1.0.0 $id

# Kill the remote server.
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the remote should stop"

let output = tg pull $id | complete
failure $output
# The local destination can return its header before the source fails, moving the same error into the stream.
let stderr = $output.stderr | str replace --regex '-> failed to pull\n-> the request failed\n   status = 500 Internal Server Error\n-> failed to start the pull\n' ''
snapshot --normalize-ids --redact [$remote.url $remote.directory ($remote.directory | path expand)] $stderr '
	error an error occurred
	-> failed to create the source stream
	-> failed to send the request
	-> failed to resolve the socket path
	   path = <redacted>/socket
	-> No such file or directory (os error 2)

'
