use ../../util/test.nu *

# A spawn whose host this server's runner cannot run goes through the scheduler, which rejects it when no runner has the host.

let server = server spawn --config {
	scheduler: { unplaceable_timeout: 0 },
}

let path = artifact {
	tangram.ts: '
		export default function () {
			return "hello";
		}

		export function child() {
			return tg.build({
				args: ["-c", "true"],
				executable: "sh",
				host: "not-a-real-host",
			});
		}

		export function command() {
			return tg.command({
				args: ["-c", "true"],
				executable: "sh",
				host: "not-a-real-host",
			});
		}
	',
}

# A top-level build with a foreign host is rejected.
let output = tg --url $server.url build --host not-a-real-host $path | complete
failure $output "a build with a foreign host should be rejected"
assert ($output.stderr | str contains "no runners available")
assert not ($output.stderr | str contains "cannot run process with host")

# A child with a foreign host spawned from inside a process is rejected.
let output = tg --url $server.url build $"($path)#child" | complete
failure $output "a child with a foreign host should be rejected"
assert ($output.stderr | str contains "no runners available")
assert not ($output.stderr | str contains "cannot run process with host")

# A spawn of a command given by ID with a foreign host is rejected.
let command = tg --url $server.url build $"($path)#command" | str trim | split row '?' | first
let socket = $server.url | str replace 'http+unix://' '' | url decode
let arg = {
	cached: false,
	command: { node: $command },
	sandbox: {},
	stdin: 'null',
	stdout: log,
	stderr: log,
} | to json --raw
let events = http post --raw --max-time 30sec --unix-socket $socket --headers { 'Content-Type': 'application/json' } 'http://localhost/processes/spawn' $arg
assert ($events | str contains "no runners available")
assert not ($events | str contains "cannot run process with host")
