use ../../util/test.nu *

# A sandbox that was running when its server was terminated is stopped after the server restarts, and it starts again with its files.

let local = server spawn

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

let sandbox = tg sandbox create --no-tokens | referent node
success (tg run $'--sandbox=($sandbox)' $write | complete) "the write should succeed"

# Terminate the server, then start it again.
kill --signal 15 $local.pid
wait_for_exit $local.pid
let local = server start $local

assert equal (tg sandbox get $sandbox | from json | get data.status) stopped "the sandbox should be stopped after the restart"
tg sandbox start $sandbox
let output = tg run $'--sandbox=($sandbox)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains "hello") "the file should survive the restart"
