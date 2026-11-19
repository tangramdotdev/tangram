use ../../util/test.nu *

# A run without a location that this server's scheduler declines runs on a remote, and its stdin, stdout, stderr, and exit code reach the client.

# Create a remote that runs the host, and a server without a runner whose scheduler accepts no host.
let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },
	roles: [api indexer scheduler],
	scheduler: { hosts: [], unplaceable_timeout: null },
}

# The stdout and stderr of the process reach the client.
let output = tg --url $local.url run --executable /bin/sh -- -c "echo out; echo err 1>&2" | complete
success $output "the run should succeed on the remote"
assert equal $output.stdout "out\n"
assert (($output.stderr | lines | last) == "err") "the stderr should reach the client"

# The client's stdin reaches the process.
let output = "hello\n" | tg --url $local.url run --executable /bin/sh -- -c "cat" | complete
success $output "the run should succeed on the remote"
assert equal $output.stdout "hello\n"

# The exit code of the process is the exit code of the run.
let output = tg --url $local.url run --executable /bin/sh -- -c "exit 3" | complete
assert equal $output.exit_code 3 "the run should exit with the code of the process"
