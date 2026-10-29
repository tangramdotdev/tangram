use ../../util/test.nu *

# Omitted resources are unlimited, configured defaults are enforced, and explicit requests take precedence.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux cgroups' }
let cpus = ^nproc | str trim | into int
if $cpus < 3 { skip_test 'this test requires at least three CPUs' }

let module = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string) => tg.command({
			executable: "/bin/sh",
			args: ["-c", script],
		}).env(tg.build(busybox));
	',
}

for case in [
	{ scheduler: {}, cpu: $cpus, memory: null },
	{ scheduler: { default_cpu: 1 }, cpu: 1, memory: null },
	{ scheduler: { default_memory: 268435456 }, cpu: $cpus, memory: 268435456 },
	{ scheduler: { default_cpu: 1, default_memory: 268435456 }, cpu: 1, memory: 268435456 },
] {
	let local = server spawn --busybox --config {
		runner: { backend: { kind: builtin }, sandbox_pool_size: 0 },
		scheduler: $case.scheduler,
	}
	let nproc = tg build $module --arg-string 'nproc' | str trim

	for explicit in [false true] {
		let args = if $explicit { [--cpu 2 --memory 134217728] } else { [] }
		let cpu = if $explicit { 2 } else { $case.cpu }
		let memory = if $explicit { 134217728 } else { $case.memory }

		# Check the number of CPUs available to a process in the sandbox.
		let sandbox = tg sandbox create --no-tokens --no-ttl ...$args | referent node
		let output = tg run --no-tokens $'--sandbox=($sandbox)' $nproc | complete
		success $output 'the process should report its CPUs'
		assert equal ($output.stdout | str trim | into int) $cpu 'the process should see the expected number of CPUs'
		tg sandbox destroy $sandbox
		tg sandbox wait $sandbox | ignore

		# Allocate more memory than the expected limit, or a large amount of memory when the sandbox is unlimited.
		let size = if $memory == null { 402653184 } else { $memory * 3 // 2 }
		let allocate = tg build $module --arg-string $'dd if=/dev/zero of=/dev/null bs=($size) count=1' | str trim
		let sandbox = tg sandbox create --no-tokens --no-ttl ...$args | referent node
		let process = tg spawn --no-tokens $'--sandbox=($sandbox)' $allocate | referent node
		let output = tg wait $process | complete
		success $output 'the process should finish'
		let outcome = $output.stdout | from json
		if $memory == null {
			assert equal $outcome.exit 0 'an unlimited sandbox should allow the allocation'
		} else {
			let error = tg get --depth inf --no-tokens $outcome.error
			assert ($error | str contains 'the sandbox exceeded its memory max') 'the sandbox should not allow an allocation beyond its memory max'
		}
		tg sandbox destroy $sandbox | complete | ignore
		tg sandbox wait $sandbox | ignore
	}
	server stop $local
}
