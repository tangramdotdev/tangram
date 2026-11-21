use ../../util/test.nu *

# Of a start of a stopped sandbox and the expiry of its ttl, exactly one changes the stopped sandbox. If the start is first, the sandbox starts with its files and is not destroyed. If the expiry is first, the sandbox is destroyed and the start fails.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true },
	runner: { stopped_sandbox_interval: 1 },
}

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

# Create a sandbox with a ttl and a file, stop it, and hold its expiry and a start just before each changes the sandbox in the index.
def hold [server: record, write: string] {
	let url = $server.url
	let sandbox = tg sandbox create --wait --no-tokens --ttl 5s | referent node
	success (tg run --no-tokens $'--sandbox=($sandbox)' $write | complete) "the write should succeed"
	tg sandbox stop --wait $sandbox
	let destroy_reserve = checkpoint watch sandbox.destroy.reserve --params { sandbox: $sandbox }
	let start_reserve = checkpoint watch sandbox.start.reserve --params { sandbox: $sandbox }
	advance_time $server 6sec
	checkpoint wait $destroy_reserve | ignore
	let start = job spawn {
		let id = job id
		timeout 30s tg --url $url sandbox start --wait $sandbox | complete | job send --tag $id 0
	}
	checkpoint wait $start_reserve | ignore
	{ destroy_reserve: $destroy_reserve, sandbox: $sandbox, start: $start, start_reserve: $start_reserve }
}

# The start is first, so the sandbox starts with its files, and the expiry, which finishes with the runner's update of its stopped sandboxes, leaves it alone.
let held = hold $local $write
let sandbox = $held.sandbox
checkpoint continue $held.start_reserve
success (job recv --tag $held.start --timeout 30sec) "the start should succeed"
let updated = checkpoint watch runner.sandbox.stopped.updated
checkpoint continue $held.destroy_reserve
checkpoint pass $updated | ignore
checkpoint unwatch $updated
checkpoint unwatch $held.start_reserve
checkpoint unwatch $held.destroy_reserve
let output = tg run --no-tokens $'--sandbox=($sandbox)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains 'hello') "the file should survive the expiry"
assert equal (tg sandbox get $sandbox | from json | get data.status) started

# The expiry is first, so the sandbox is destroyed, and the start fails.
let held = hold $local $write
let sandbox = $held.sandbox
checkpoint continue $held.destroy_reserve
success (timeout 30s tg sandbox wait $sandbox | complete) "the sandbox should be destroyed"
checkpoint continue $held.start_reserve
let output = job recv --tag $held.start --timeout 30sec
failure $output "the start should fail after the expiry"
assert ($output.stderr | str contains 'the sandbox was destroyed')
checkpoint unwatch $held.start_reserve
checkpoint unwatch $held.destroy_reserve
assert equal (tg sandbox get $sandbox | from json | get data.status) destroyed
failure (tg run --no-tokens $'--sandbox=($sandbox)' $read | complete) "a process should not run in the destroyed sandbox"
