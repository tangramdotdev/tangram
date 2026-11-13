use ../../util/test.nu *

# Configure nondefault resources, hostnames, mounts, and networking before using a warm sandbox.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux namespaces'
}
if not ('/usr/bin/python3' | path exists) {
	skip_test 'this test requires the host programs in /usr/bin'
}
if (^nproc | str trim | into int) < 2 {
	skip_test 'this test requires at least two CPUs'
}
let hostname = ^uname -n | str trim
let backends = if (which bwrap | is-empty) { [builtin] } else { [builtin bubblewrap] }
for backend in $backends {
	for bounds in [
		{ warm_cpu: 1, warm_memory: 134217728, cpu: 2, memory: 268435456 },
		{ warm_cpu: 2, warm_memory: 268435456, cpu: 1, memory: 134217728 },
	] {
		let local = server spawn --config {
			advanced: { checkpoints: true },
			runner: { backend: { kind: $backend }, sandbox_pool_size: 1 },
			scheduler: { default_cpu: $bounds.warm_cpu, default_memory: $bounds.warm_memory },
		}
		let source = $'source-($backend)-($bounds.cpu)' | path expand
		mkdir $source
		"hello\n" | save ($source | path join file)
		for name in [passwd nsswitch.conf resolv.conf] {
			'' | save ($source | path join $name)
		}
		let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten
		let script = r#'
			set -eu
			export PATH=/usr/bin:/bin
			uname -n
			python3 -c 'import os; print(len(os.sched_getaffinity(0)))'
			cat /target/file
			test -s /etc/passwd
			test "$(cat /etc/file)" = hello
			if (echo changed > /target/file) 2>/dev/null; then exit 1; fi
			if ip -4 -o addr show scope global | grep -q inet; then
				test -s /etc/resolv.conf
				echo connected
			else
				echo isolated
			fi
		'#

		for connected in [false true] {
			let network = if $connected { [--network] } else { [] }
			let checkpoint = checkpoint watch runner.sandbox.pool.take
			let job = job spawn {
				let job_id = job id
				tg sandbox create --no-tokens --no-ttl --cpu-min 0.25 --cpu $bounds.cpu --memory-min 33554432 --memory $bounds.memory --hostname configured --mount $'($source):/target,ro' --mount $'($source):/etc,ro' ...$mounts ...$network | job send --tag $job_id 0
			}
			checkpoint pass $checkpoint | ignore
			checkpoint unwatch $checkpoint
			let sandbox = job recv --tag $job | referent node
			let output = tg run --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c $script | complete
			success $output 'the configured sandbox should run a workload'
			let network = if $connected { 'connected' } else { 'isolated' }
			assert equal $output.stdout $"configured\n($bounds.cpu)\nhello\n($network)\n"
			assert equal (open --raw ($source | path join file)) "hello\n"
			assert equal (^uname -n | str trim) $hostname

			# The requested memory maximum applies to processes in the claimed sandbox.
			let process = tg spawn --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'export PATH=/usr/bin:/bin; dd if=/dev/zero of=/dev/null bs=402653184 count=1' | referent node
			let output = tg wait $process | from json
			let error = tg get --depth inf --no-tokens $output.error
			assert ($error | str contains 'the sandbox exceeded its memory max')
			tg sandbox destroy $sandbox | complete | ignore
			tg sandbox wait $sandbox | ignore
			let usage = tg sandbox get $sandbox | from json | get data.usage
			assert ($usage.cpu.reservation > 0) 'accounting should use the requested CPU minimum'
			assert ($usage.memory.reservation > 0) 'accounting should use the requested memory minimum'
		}
		server stop $local
	}
}
