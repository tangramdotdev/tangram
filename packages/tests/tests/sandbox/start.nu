use ../../util/test.nu *

# A stopped sandbox starts again with its files and runs new processes.

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

tg sandbox stop $sandbox
tg sandbox start $sandbox
assert equal (tg sandbox get $sandbox | from json | get data.status) started "the sandbox should be started"

# A process spawned as soon as the start returns reads the file written before the stop.
let output = tg run $'--sandbox=($sandbox)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains "hello") "the file should survive the stop"

# Starting a started sandbox succeeds.
tg sandbox start $sandbox
