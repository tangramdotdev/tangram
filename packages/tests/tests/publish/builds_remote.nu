use ../../util/test.nu *

# Publishing a package whose build runs on a remote, because the local scheduler accepts no host, tags the remote's process on the local server and on the publish destination.

let host = current_host
let path = artifact {
	tangram.ts: '
		export default function () { return "default"; }

		export let metadata = {
			tag: "builds-remote/1.0.0",
			builds: true,
		};
	'
}
let tag = $'builds-remote/builds/1.0.0/default/($host)'

# Create a remote that runs the host, and a server without a runner whose scheduler accepts no host.
let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },
	roles: [api indexer scheduler],
	scheduler: { hosts: [], unplaceable_timeout: null },
}

# The publish builds the default export on the remote, and tags its process on both servers.
let output = tg --url $local.url publish --host $host $path | complete
success $output "the publish should succeed"
let remote_target = tg --url $remote.url tag get $tag | from json | get target.id
assert ($remote_target | str starts-with 'pcs_') "the remote build tag should target a process"
assert equal (tg --url $remote.url process output $remote_target | from json) "default"
let local_target = tg --url $local.url tag get $tag | from json | get target.id
assert equal $local_target $remote_target "the local build tag should target the same process"

# Create a publish destination that accepts no host, and another remote that runs the host.
let runner = server spawn --name runner
let destination = server spawn --name destination --config {
	roles: [api indexer scheduler],
	scheduler: { hosts: [], unplaceable_timeout: null },
}
let local = server spawn --name local2 --config {
	remotes: {
		default: { url: $destination.url },
		runner: { url: $runner.url },
	},
	roles: [api indexer scheduler],
	scheduler: { hosts: [], unplaceable_timeout: null },
}

# The publish builds the default export on the other remote, and pushes its process to the destination.
let output = tg --url $local.url publish --host $host $path | complete
success $output "the publish should succeed"
let destination_target = tg --url $destination.url tag get $tag | from json | get target.id
assert ($destination_target | str starts-with 'pcs_') "the destination build tag should target a process"
assert equal (tg --url $destination.url process output $destination_target | from json) "default"
let local_target = tg --url $local.url tag get $tag | from json | get target.id
assert equal $local_target $destination_target "the local build tag should target the same process"
