use ../../test.nu *

# Publishing fails promptly when the remote server is not running.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let path = artifact {
	tangram.ts: '
		export default () => "Hello, World!";

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
assert ($output.stderr | str contains 'failed to') "the error should mention the failure"
