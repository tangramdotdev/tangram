use ../../util/test.nu *

# A build's sandbox with a ttl accepts runs after the build finishes, and it is destroyed once it is idle for the ttl.

let server = server spawn --now '2026-01-01T00:00:00Z'

let path = artifact {
	tangram.ts: '
		export function write() {
			return tg.command({
				args: ["-ec", "echo hello > /tmp/file"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
		export function read() {
			return tg.command({
				args: ["-ec", "read -r line < /tmp/file; echo \"$line\""],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	',
}
let write = tg build $'($path)#write' | str trim
let read = tg build $'($path)#read' | str trim

# Build with a ttl, then run in the build's sandbox.
let process = tg build --no-tokens --detach --ttl 1min $write | referent node
success (tg process wait $process | complete) "the build should succeed"
let sandbox = tg get $process | from json | get sandbox
let output = tg run $'--sandbox=($sandbox)' $read | complete
success $output "the run in the build's sandbox should succeed"
assert ($output.stdout | str contains "hello") "the run should see the build's file"

# Advance past the ttl, and the sandbox is destroyed.
advance_time $server 2min
success (timeout 10s tg sandbox wait $sandbox | complete) "the sandbox should be destroyed after the ttl"
assert equal (tg sandbox get $sandbox | from json | get data.status) destroyed
