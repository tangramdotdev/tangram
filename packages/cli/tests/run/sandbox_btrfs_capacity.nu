use ../lib/test.nu *

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let root = $env.TANGRAM_TEST_BTRFS_PATH? | default ''
if ($root | is-empty) {
	skip_test 'TANGRAM_TEST_BTRFS_PATH must name a Btrfs filesystem with quotas enabled'
}

container_cgroup_parent [cpu memory] | ignore
let directory = mktemp --directory --tmpdir-path $root
let project_start = (random int 100000..10000000) * 100
let local = server spawn --directory $directory --config {
	runner: {
		backend: { kind: builtin, storage_limits: true },
		overcommit: { storage: 2 },
		resources: { filesystem_project_ids: { start: $project_start, count: 2 } },
		sandbox_pool_size: 0,
		storage: 16777216,
	},
}
let mounts = ['/usr' '/lib'] | where { $in | path exists } | each { |path| ['--mount' $'($path | path expand):($path),ro'] } | flatten
let first = tg sandbox create --no-tokens ...$mounts --storage 16777216 | referent node
let output = tg run $'--sandbox=($first)' --executable /usr/bin/true | complete
success $output 'the first sandbox should reserve the pool without filling its quota'

# A low usage reading must not lend the first sandbox's granted capacity to a second sandbox.
tg sandbox create --no-tokens ...$mounts --storage-min 8388608 --storage 8388608 | ignore
mut rejected = false
for attempt in 0..100 {
	if (open --raw $local.log | str contains 'not enough backed resource capacity') {
		$rejected = true
		break
	}
	sleep 100ms
}
assert $rejected 'a live Btrfs sandbox must retain its full storage backing'

tg sandbox destroy $first
tg sandbox wait $first | ignore
server stop $local
btrfs filesystem sync $root | ignore
btrfs subvolume sync $root | ignore
btrfs qgroup destroy $'1/($project_start)' $root
chmod -R u+rwX $directory
rm -rf $directory
