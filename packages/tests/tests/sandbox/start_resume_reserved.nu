use ../../util/test.nu *

# A start that reserves the next boot of a stopped sandbox and ends before it enqueues the boot leaves a reservation that the next start finishes, so the sandbox starts at its next boot with its files.

let local = server spawn --config {
	advanced: { checkpoints: true },
}
let socket = $local.url | str replace 'http+unix://' '' | url decode

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

# Create a sandbox with a file, and stop it.
let sandbox = tg sandbox create --wait --no-tokens | referent node
success (tg run --no-tokens $'--sandbox=($sandbox)' $write | complete) "the write should succeed"
tg sandbox stop --wait $sandbox

# Start the sandbox, hold the start after it reserves the next boot and before it enqueues the boot, and abandon the start.
let enqueue = checkpoint watch sandbox.start.enqueue --params { sandbox: $sandbox }
let start = job spawn {
	let id = job id
	try { http post --allow-errors --max-time 3sec --unix-socket $socket --content-type application/json $'http://localhost/sandboxes/($sandbox)/start' '{}' } catch { null } | ignore
	0 | job send --tag $id 0
}
checkpoint wait $enqueue | ignore
job recv --tag $start --timeout 30sec | ignore
checkpoint unwatch $enqueue

# Start the sandbox again. The start finishes the reserved boot.
success (timeout 30s tg sandbox start --wait $sandbox | complete) "the start should finish the reserved boot"
let data = tg sandbox get $sandbox | from json | get data
assert equal $data.boot 1
assert equal $data.status started
let output = tg run --no-tokens $'--sandbox=($sandbox)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains 'hello') "the file should survive the abandoned start"
tg sandbox destroy $sandbox
