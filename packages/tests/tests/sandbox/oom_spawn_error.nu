use ../../util/test.nu *

# A process whose spawn finds its sandbox killed for exceeding its memory max reports the memory max.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux cgroups' }

let local = server spawn --busybox --config {
	advanced: { checkpoints: true },
	runner: { backend: { kind: builtin }, sandbox_pool_size: 0 },
}
let module = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string) => tg.command({
			executable: "/bin/sh",
			args: ["-c", script],
		}).env(tg.build(busybox));
	',
}
let allocate = tg build $module --arg-string 'dd if=/dev/zero of=/dev/null bs=201326592 count=1' | str trim
let succeed = tg build $module --arg-string 'true' | str trim
let sandbox = tg sandbox create --no-tokens --no-time-to-live --memory 134217728 | referent node

# Hold a process just before the runner spawns it in the sandbox.
let start = checkpoint watch runner.process.start
let held = tg spawn --no-tokens $'--sandbox=($sandbox)' $succeed | referent node
checkpoint wait $start 0 | ignore

# Exceed the sandbox's memory max with another process, which kills the sandbox.
let process = tg spawn --no-tokens $'--sandbox=($sandbox)' $allocate | referent node
checkpoint pass $start 1 | ignore
let output = tg wait $process | complete
success $output 'the allocating process should finish'

# Release the held process, whose spawn fails because the sandbox was killed.
checkpoint continue $start 0
checkpoint unwatch $start
let output = tg wait $held | complete
success $output 'the held process should finish'
let outcome = $output.stdout | from json
assert ($outcome.exit != 0) 'the held process should fail'
let error = tg get --depth inf --no-tokens $outcome.error
assert ($error | str contains 'the sandbox exceeded its memory max') 'the held process should report the memory max'
