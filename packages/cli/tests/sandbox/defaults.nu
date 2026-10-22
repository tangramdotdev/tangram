use ../lib/test.nu *

# Omitted resources are unlimited, configured defaults are enforced, and explicit requests take precedence.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux cgroups' }
if (id -u | into int) == 0 { skip_test 'this test requires unprivileged pasta networking' }
if (which pasta | is-empty) { skip_test 'this test requires pasta' }

for case in [
	{ scheduler: {}, cpu: max, memory: max },
	{ scheduler: { default_cpu: 1 }, cpu: '1000000', memory: max },
	{ scheduler: { default_memory: 268435456 }, cpu: max, memory: '268435456' },
	{ scheduler: { default_cpu: 1, default_memory: 268435456 }, cpu: '1000000', memory: '268435456' },
] {
	let local = server spawn --config {
		runner: { backend: { kind: builtin }, sandbox_pool_size: 0 },
		scheduler: $case.scheduler,
	}
	let server_pid = glob ($local.directory | path join '**/lock')
		| each { |path| open --raw $path | str trim }
		| where { |pid| (^ps -o command= -p $pid | complete).stdout | str contains $local.directory }
		| first

	for explicit in [false true] {
		let args = if $explicit { [--cpu '0.5' --memory 134217728] } else { [] }
		let sandbox = tg sandbox create --network --no-tokens --no-ttl ...$args | referent node
		# Wait for the runner to finish starting the sandbox, because the sandbox can connect before its pasta helper starts.
		wait_for_log $local $'the sandbox is ready, sandbox=($sandbox)'
		let helpers = ^pgrep -P $server_pid -f '(^|/)pasta([.[:space:]]|$)' | lines
		assert equal ($helpers | length) 1
		let helper = $helpers | first
		let cgroup = open --raw $'/proc/($helper)/cgroup' | str trim | str replace '0::' '' | path dirname
		let path = $'/sys/fs/cgroup($cgroup)'
		let cpu = if ($path | path join cpu.max | path exists) { open --raw ($path | path join cpu.max) | split row ' ' | first } else { max }
		let memory = open --raw ($path | path join memory.max) | str trim
		assert equal $cpu (if $explicit { '500000' } else { $case.cpu })
		if not $explicit and $case.cpu == max and ($path | path join cpuset.cpus | path exists) {
			assert equal (open --raw ($path | path join cpuset.cpus) | str trim) '' 'an unlimited sandbox must not be pinned to a subset of CPUs'
		}
		assert equal $memory (if $explicit { '134217728' } else { $case.memory })
		tg sandbox destroy $sandbox
		tg sandbox wait $sandbox | ignore
	}
	server stop $local
}
