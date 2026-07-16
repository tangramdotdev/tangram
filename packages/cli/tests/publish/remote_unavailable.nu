use ../../test.nu *

# Publishing fails promptly when the remote server is not running.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let path = artifact {
	tangram.ts: '
		export default function () { return "Hello, World!"; }

		export let metadata = {
			tag: "test-pkg/1.0.0",
		};
	'
}

# Kill the remote server.
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the remote should stop"

let output = tg publish $path | complete
failure $output
snapshot --normalize --redact [$path $remote.url $remote.directory ($remote.directory | path expand)] $output.stderr '
	error an error occurred
	-> failed to push items
	-> failed to create the pull stream
	-> failed to sync
	   remote = default
	-> failed to send the request
	-> failed to resolve the socket path
	   path = <redacted>/socket
	-> No such file or directory (os error 2)

'
