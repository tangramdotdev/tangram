use ../../util/test.nu *

# A scheduler declines a sandbox whose host is not in its hosts, so that a build or a sandbox create with that host fails at once with an error that names the host.

let server = server spawn --config {
	scheduler: { unplaceable_timeout: null },
}

let path = artifact {
	tangram.ts: '
		export default function () {
			return "hello";
		}
	',
}

# A build with a host that the scheduler does not accept fails.
let output = tg --url $server.url build --host not-a-real-host $path | complete
failure $output "a build with a host that no scheduler accepts should fail"
assert ($output.stderr | str contains "no location accepts the process's host")
assert ($output.stderr | str contains "not-a-real-host")

# A sandbox create with a host that the scheduler does not accept fails.
let output = tg --url $server.url sandbox create --no-tokens --host not-a-real-host | complete
failure $output "a sandbox create with a host that no scheduler accepts should fail"
assert ($output.stderr | str contains "no location accepts the sandbox's host")
assert ($output.stderr | str contains "not-a-real-host")

# A sandbox with the server's own host is still accepted.
let output = tg --url $server.url sandbox create --no-tokens | complete
success $output "a sandbox create with the server's host should succeed"
