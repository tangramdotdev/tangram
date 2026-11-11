use ../../util/test.nu *

# A get of a stopped sandbox whose next boot is reserved reports the next boot, although this server's runner still has the stopped boot, which a process that ran in it retains.

let local = server spawn --config {
	advanced: { checkpoints: true },
}

let path = artifact {
	tangram.ts: '
		export default () => {
			return tg.command({
				args: ["-ec", "true"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	',
}
let command = tg build $path | str trim

# Create a sandbox, run a process in it, and stop it.
let sandbox = tg sandbox create --wait --no-tokens | referent node
success (tg run --no-tokens $'--sandbox=($sandbox)' $command | complete) "the process should succeed"
tg sandbox stop --wait $sandbox

# Start the sandbox without waiting, and hold the offer of its next boot at the runner.
let create = checkpoint watch runner.control.create_sandbox --params { sandbox: $sandbox }
tg sandbox start $sandbox
checkpoint wait $create | ignore

# The get reports the next boot, which is starting.
let data = tg sandbox get $sandbox | from json | get data
assert equal ($data.boot? | default 0) 1 "the get should report the next boot"
assert equal $data.status starting

# Let the boot start, and destroy the sandbox.
checkpoint unwatch $create
success (timeout 30s tg sandbox start --wait $sandbox | complete) "the start should finish"
tg sandbox destroy $sandbox
