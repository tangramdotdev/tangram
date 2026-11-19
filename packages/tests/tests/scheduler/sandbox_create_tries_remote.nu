use ../../util/test.nu *

# A sandbox create without a location tries the remotes when this server's scheduler does not accept its host, a create with an explicit location tries only that location, and a create that no location accepts fails with an error that names the host.

# Create a remote that runs the host, and a server without a runner whose scheduler accepts no host.
let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },
	roles: [api indexer scheduler],
	scheduler: { hosts: [], unplaceable_timeout: null },
}

# A create without a location creates the sandbox on the remote, where it starts.
let output = tg --url $local.url sandbox create --wait | complete
success $output "the create should create the sandbox on the remote"
let sandbox = $output.stdout | str trim | split row '?' | first
let data = tg --url $remote.url sandbox get $sandbox | from json
assert equal $data.data.status 'started'

# A create at this server fails, because its scheduler does not accept the host.
let output = tg --url $local.url sandbox create --local | complete
failure $output "a create at this server should fail"
assert ($output.stderr | str contains (current_host))

# A create that no location accepts fails.
let alone = server spawn --name alone --config {
	roles: [api indexer scheduler],
	scheduler: { hosts: [], unplaceable_timeout: null },
}
let output = tg --url $alone.url sandbox create | complete
failure $output "a create that no location accepts should fail"
assert ($output.stderr | str contains (current_host))
