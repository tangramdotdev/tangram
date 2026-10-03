use ../lib/test.nu *

let host = $'(^uname -m | str trim | str replace arm64 aarch64)-(if $nu.os-info.name == macos { 'darwin' } else { $nu.os-info.name })'
let remote = server spawn --name remote
let local = server spawn --name local --config {
	process: { spawn: { host: 'forwarder-default-must-not-be-used' } },
	remotes: { default: { url: $remote.url } },
}
let path = artifact {
	tangram.ts: '
		export default async () => {
			const child = await tg.build(childHost);
			return { parent: tg.host.current, child };
		};
		export const childHost = () => tg.host.current;
	',
}
let output = timeout 30s tg --url $local.url build --remote --cached=false $path | from json
assert equal $output { parent: $host, child: $host }

# A destination default must not replace an inherited or explicit host.
let inherited = server spawn --name inherited --config {
	process: { spawn: { host: 'destination-default-must-not-be-used' } },
}
let output = timeout 30s tg --url $inherited.url build --host $host --cached=false $path | from json
assert equal $output { parent: $host, child: $host }

# Direct spawn uses the same forwarding rules as a process connection.
let socket = $local.url | str replace 'http+unix://' '' | url decode
let arg = {
	cached: false,
	command: { node: { executable: { node: { path: sh } }, args: [{ kind: string, value: '-c' } { kind: string, value: 'printf hello' }] } },
	location: remote,
	sandbox: {},
	stdin: 'null',
	stdout: log,
	stderr: log,
} | to json --raw
let events = http post --raw --max-time 30sec --unix-socket $socket --headers { 'Content-Type': 'application/json' } 'http://localhost/processes/spawn' $arg
assert (not ($events | str contains 'event: error'))
let output = $events | lines | where { $in starts-with 'data: ' } | last | str substring 6.. | from json
let data = tg --url $remote.url process get $output.process | from json
assert equal $data.command.node.host $host
assert ($output.command | str starts-with 'cmd_')

# Cross-region forwarding also leaves the host unspecified until the destination.
let instance = instance --primary-region east --regions [{ name: east } { name: west }]
let east = server spawn --instance $instance --region east --url (instance region url $instance east) --name east
let west = server spawn --instance $instance --region west --directory (mktemp -d) --name west --config {
	process: { spawn: { host: 'region-forwarder-default-must-not-be-used' } },
}
let output = timeout 30s tg --url $west.url build --location 'local(east)' --cached=false $path | from json
assert equal $output { parent: $host, child: $host }
