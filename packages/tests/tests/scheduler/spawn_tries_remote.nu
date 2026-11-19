use ../../util/test.nu *

# A spawn without a location tries the remotes when this server's scheduler does not accept its host, a spawn with an explicit location tries only that location, and a spawn that no location accepts fails with an error that names the host.

let path = artifact {
	tangram.ts: '
		export default function () {
			return "hello";
		}
	',
}

# Create a remote that runs the host, and a server without a runner whose scheduler accepts no host.
let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },
	roles: [api indexer scheduler],
	scheduler: { hosts: [], unplaceable_timeout: null },
}

# A build without a location runs on the remote.
let output = tg --url $local.url build $path | complete
success $output "the build should run on the remote"
assert equal ($output.stdout | str trim) '"hello"'

# A build at this server fails, because its scheduler does not accept the host.
let path = artifact {
	tangram.ts: '
		export default function () {
			return "local";
		}
	',
}
let output = tg --url $local.url build --local $path | complete
failure $output "a build at this server should fail"
assert ($output.stderr | str contains (current_host))

# A build that no location accepts fails.
let alone = server spawn --name alone --config {
	roles: [api indexer scheduler],
	scheduler: { hosts: [], unplaceable_timeout: null },
}
let output = tg --url $alone.url build $path | complete
failure $output "a build that no location accepts should fail"
assert ($output.stderr | str contains (current_host))
