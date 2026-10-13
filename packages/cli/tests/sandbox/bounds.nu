use ../lib/test.nu *

let quota_root = $env.TANGRAM_TEST_PROJECT_QUOTA_PATH? | default ''
if $nu.os-info.name == 'linux' and ($quota_root | is-empty) {
	skip_test 'TANGRAM_TEST_PROJECT_QUOTA_PATH is required for sandbox storage accounting'
}

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
let reserved = $cpus | first
let siblings = cpu_list (open --raw $'/sys/devices/system/cpu/cpu($reserved)/topology/thread_siblings_list')
if not ($siblings | all { $in in $cpus }) {
	skip_test 'this test requires every SMT sibling of a reserved core'
}
let other = $cpus | where { $in not-in $siblings }
if ( $other | is-empty) {
	skip_test 'this test requires a another CPU outside the reserved core'
}
let other_core =  $other | first
let other_siblings = cpu_list (open --raw $'/sys/devices/system/cpu/cpu($other_core)/topology/thread_siblings_list')
if not ($other_siblings | all { $in in $cpus }) {
	skip_test 'this test requires every SMT sibling of the other core'
}
let cpus = $siblings | append $other_siblings | uniq | sort | str join ','
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

let directory = mktemp --directory --tmpdir-path $quota_root
let local = try { server spawn --directory $directory --config {
	runner: {
		cpu_pool: $pool,
		overcommit: { cpu: 4 },
		memory_sampling_interval: 0.05,
		sandbox_pool_size: 0,
	},
} } catch { |error|
	^rmdir $pool
	error make $error
}

try {
	let sandbox = tg sandbox create --no-tokens --no-ttl --cpu-min 0.25 --cpu-max 1 | referent node
	sleep 200ms
	tg sandbox destroy $sandbox
	let data = tg sandbox get $sandbox | from json | get data
	assert equal $data.cpu { max: 1, min: 0.25 }
	assert ($data.usage.cpu.reservation >= 50) 'an idle minimum must accrue reservation time'
	assert ($data.usage.cpu.consumption < 200) 'idle capacity above the minimum must not accrue consumption time'
} catch { |error|
	server stop $local
	^rmdir $pool
	error make $error
}

server stop $local
^rmdir $pool
