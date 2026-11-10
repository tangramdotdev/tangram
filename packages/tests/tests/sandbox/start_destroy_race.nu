use ../../util/test.nu *

# Of a start and a destroy of a stopped sandbox, exactly one changes the stopped sandbox. If the destroy is first, the start fails. If the start is first, the sandbox starts with its files, and the destroy then destroys the started sandbox.

let local = server spawn --config {
	advanced: { checkpoints: true },
}
let url = $local.url

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
def stopped_sandbox [write: string] {
	let sandbox = tg sandbox create --wait --no-tokens | referent node
	success (tg run --no-tokens $'--sandbox=($sandbox)' $write | complete) "the write should succeed"
	tg sandbox stop --wait $sandbox
	$sandbox
}

# Hold a start and a destroy of a stopped sandbox just before each changes the sandbox in the index.
def hold [url: string, sandbox: string, --wait] {
	let flags = if $wait { ['--wait'] } else { [] }
	let start_reserve = checkpoint watch sandbox.start.reserve --params { sandbox: $sandbox }
	let destroy_reserve = checkpoint watch sandbox.destroy.reserve --params { sandbox: $sandbox }
	let start = job spawn {
		let id = job id
		timeout 30s tg --url $url sandbox start ...$flags $sandbox | complete | job send --tag $id 0
	}
	let destroy = job spawn {
		let id = job id
		timeout 30s tg --url $url sandbox destroy $sandbox | complete | job send --tag $id 0
	}
	checkpoint wait $start_reserve | ignore
	checkpoint wait $destroy_reserve | ignore
	{ destroy: $destroy, destroy_reserve: $destroy_reserve, start: $start, start_reserve: $start_reserve }
}

# The destroy is first, so the start fails.
let sandbox = stopped_sandbox $write
let held = hold $url $sandbox
checkpoint continue $held.destroy_reserve
success (job recv --tag $held.destroy --timeout 30sec) "the destroy should succeed"
checkpoint continue $held.start_reserve
let output = job recv --tag $held.start --timeout 30sec
failure $output "the start should fail after the destroy"
assert ($output.stderr | str contains 'the sandbox was destroyed')
checkpoint unwatch $held.start_reserve
checkpoint unwatch $held.destroy_reserve
success (timeout 30s tg sandbox destroy --wait $sandbox | complete) "the sandbox should be destroyed"
assert equal (tg sandbox get $sandbox | from json | get data.status) destroyed

# The start is first, so the sandbox starts with its files, and the destroy then destroys it.
let sandbox = stopped_sandbox $write
let held = hold --wait $url $sandbox
checkpoint continue $held.start_reserve
success (job recv --tag $held.start --timeout 30sec) "the start should succeed"
let output = tg run --no-tokens $'--sandbox=($sandbox)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains 'hello') "the file should survive the start"
checkpoint continue $held.destroy_reserve
success (job recv --tag $held.destroy --timeout 30sec) "the destroy should succeed"
checkpoint unwatch $held.start_reserve
checkpoint unwatch $held.destroy_reserve
success (timeout 30s tg sandbox destroy --wait $sandbox | complete) "the sandbox should be destroyed"
assert equal (tg sandbox get $sandbox | from json | get data.status) destroyed

# Release a start and a destroy together. Either the start fails because the destroy was first, or the start succeeds, and the sandbox ends destroyed.
let sandbox = stopped_sandbox $write
let held = hold --wait $url $sandbox
checkpoint unwatch $held.start_reserve
checkpoint unwatch $held.destroy_reserve
success (job recv --tag $held.destroy --timeout 30sec) "the destroy should succeed"
let output = job recv --tag $held.start --timeout 30sec
if $output.exit_code != 0 {
	assert ($output.stderr | str contains 'the sandbox was destroyed')
}
success (timeout 30s tg sandbox destroy --wait $sandbox | complete) "the sandbox should be destroyed"
assert equal (tg sandbox get $sandbox | from json | get data.status) destroyed
