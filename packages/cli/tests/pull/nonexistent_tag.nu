use ../../test.nu *

# Pulling a tag that exists on neither the local server nor the remote fails.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let output = tg pull nonexistent/1.0.0 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the reference
	   reference = nonexistent/1.0.0?location=remote

'
