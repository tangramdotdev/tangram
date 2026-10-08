use ../lib/test.nu *

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux cpuset partitions'
}

def cpu_list [value: string] {
	$value | str trim | split row ',' | each { |part|
		let range = $part | split row '-' | into int
		if ($range | length) == 1 { [$range.0] } else { $range.0..$range.1 | each { $in } }
	} | flatten
}

let parent = container_cgroup_parent [cpu cpuset memory pids]
let cpus = cpu_list (open --raw ($parent | path join cpuset.cpus.effective))
let dedicated = $cpus | first
let siblings = cpu_list (open --raw $'/sys/devices/system/cpu/cpu($dedicated)/topology/thread_siblings_list')
if not ($siblings | all { $in in $cpus }) {
	skip_test 'this test requires every SMT sibling of a dedicated core'
}
let shared = $cpus | where { $in not-in $siblings }
if ($shared | is-empty) {
	skip_test 'this test requires a shared CPU outside the dedicated core'
}
let cpus = $siblings | append ($shared | first) | sort | str join ','
let pool = $parent | path join $'tangram_test_((random uuid))'
mkdir $pool

try {
	open --raw ($parent | path join cpuset.mems.effective) | save --force ($pool | path join cpuset.mems)
	$cpus | save --force ($pool | path join cpuset.cpus)
	$cpus | save --force ($pool | path join cpuset.cpus.exclusive)
	'root' | save --force ($pool | path join cpuset.cpus.partition)
	if (open --raw ($pool | path join cpuset.cpus.partition) | str trim) != 'root' {
		error make { msg: 'an exclusive cpuset partition is unavailable' }
	}
	'+cpu +cpuset +memory +pids' | save --force ($pool | path join cgroup.subtree_control)
} catch {
	^rmdir $pool
	skip_test 'this test requires permission to create an exclusive cpuset partition'
}

let local = try { server spawn --config {
	runner: {
		cpu_pool: $pool,
		dedicated_cpus: [$dedicated],
		memory_sampling_interval: 0.05,
		sandbox_pool_size: 0,
	},
} } catch { |error|
	^rmdir $pool
	error make $error
}

try {
	let sandbox = tg sandbox create --no-tokens --no-ttl --dedicated-cpu 1 | referent node
	sleep 200ms
	tg sandbox destroy $sandbox
	let data = tg sandbox get $sandbox | from json | get data
	assert equal $data.cpu { dedicated: 1, shared: 0 }
	assert equal $data.usage.cpu.shared 0
	assert ($data.usage.cpu.dedicated >= 200) 'idle dedicated cores must accrue allocation time'

	# Mixed requests must either report both counters or explicitly reject missing perf permissions.
	let output = tg sandbox create --no-tokens --no-ttl --cpu 1 --dedicated-cpu 1 | complete
	if $output.exit_code == 0 {
		let sandbox = $output.stdout | referent node
		sleep 200ms
		tg sandbox destroy $sandbox
		let data = tg sandbox get $sandbox | from json | get data
		assert equal $data.cpu { dedicated: 1, shared: 1 }
		assert ($data.usage.cpu.dedicated >= 200)
		assert ($data.usage.cpu.shared < 200) 'an idle mixed sandbox must not accrue allocated shared CPU-time'
	} else {
		assert ($output.stderr | str contains 'CAP_PERFMON')
	}
} catch { |error|
	server stop $local
	^rmdir $pool
	error make $error
}

server stop $local
^rmdir $pool
