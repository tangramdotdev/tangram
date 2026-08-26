use ../../../test.nu *

# The current host matches the native architecture and operating system.

let architecture = (^uname -m | str trim | str replace arm64 aarch64)
let operating_system = if $nu.os-info.name == 'macos' { 'darwin' } else { $nu.os-info.name }
let host = $"($architecture)-($operating_system)"
let server = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.host.current;
		}
	'
}

let build = tg build --detach --verbose $path | from json
let process = tg get $build.process | from json
let command = tg get $process.command
assert (not ($command | str contains '"--host"')) "the JavaScript command must not override the runner's native host"

let output = tg output $build.process | from json
assert equal $output $host
