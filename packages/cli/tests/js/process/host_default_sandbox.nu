use ../../../test.nu *

# A process spawned from within a sandbox inherits the sandbox's host before the server default.

let architecture = (^uname -m | str trim | str replace arm64 aarch64)
let operating_system = if $nu.os-info.name == 'macos' { 'darwin' } else { $nu.os-info.name }
let host = $"($architecture)-($operating_system)"
let server = spawn --config { process: { spawn: { host: 'not-a-real-host' } } }

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn({ executable: "echo" }).sandbox();
			return (await (await process.command).host) === tg.host.current;
		}
	'
}

let output = tg build --host $host $path
snapshot $output 'true'
