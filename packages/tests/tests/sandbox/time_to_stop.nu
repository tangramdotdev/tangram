use ../../util/test.nu *

# A sandbox stops after it runs without processes for its time to stop, and it starts again with its files.

let local = server spawn --now '2026-01-01T00:00:00Z' --config { advanced: { checkpoints: true } }

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

let sandbox = tg sandbox create --no-tokens --time-to-stop 5s | referent node
let data = tg sandbox get $sandbox | from json | get data
assert equal $data.time_to_stop 5 "the sandbox should record its time to stop"
success (tg run $'--sandbox=($sandbox)' $write | complete) "the write should succeed"

# Advance the clock past the time to stop, and wait for the server to receive the stop.
let stopped = checkpoint watch sandbox.control.stop --params { sandbox: $sandbox }
advance_time $local 6sec
checkpoint pass $stopped
checkpoint unwatch $stopped
tg sandbox stop $sandbox
assert equal (tg sandbox get $sandbox | from json | get data.status) stopped "the sandbox should stop after its time to stop"

tg sandbox start $sandbox
let output = tg run $'--sandbox=($sandbox)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains "hello") "the file should survive the stop"
