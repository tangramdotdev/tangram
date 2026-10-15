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
# Quota IDs are filesystem-wide, and other runners may still have pending deletions.
let project_start = (random int 100000..10000000) * 100
let local = server spawn --directory $directory --config {
	runner: {
		backend: { kind: builtin, storage_limits: true },
		resources: { filesystem_project_ids: { start: $project_start, count: 1 } },
		sandbox_pool_size: 0,
		storage: 16777216,
	},
}
# Use host utilities without downloading a separate toolchain.
let mounts = ['/usr' '/lib'] | where { $in | path exists } | each { |path| ['--mount' $'($path | path expand):($path),ro'] } | flatten
let sandbox = tg sandbox create --no-tokens ...$mounts --storage 16777216 | referent node

let output = tg run $'--sandbox=($sandbox)' --executable /usr/bin/sh -- -c 'set -e; test "$(stat -f -c %t /tmp)" = 9123683e; dd if=/dev/urandom of=/tmp/data bs=1M count=4; echo ok' | complete
success $output 'a sandbox on Btrfs should be able to write below its storage limit'
assert equal ($output.stdout | str trim) 'ok'

let output = tg run $'--sandbox=($sandbox)' --executable /usr/bin/sh -- -c 'dd if=/dev/urandom of=/tmp/full bs=1M count=32' | complete
failure $output 'a sandbox on Btrfs must not exceed its storage limit'
assert (($output.stderr | str contains 'No space left on device') or ($output.stderr | str contains 'Disk quota exceeded'))

tg sandbox destroy $sandbox
let output = tg sandbox wait $sandbox | complete
success $output 'the Btrfs sandbox should clean up its writable filesystem'
assert equal ($output.stdout | from json) destroyed
let usage = tg sandbox get $sandbox | from json | get data.usage.storage.consumption
assert ($usage > 0) 'the Btrfs sandbox should account for its storage usage'

# Reuse the only project ID and the full pool after the kernel releases the deleted data.
btrfs filesystem sync $root | ignore
btrfs subvolume sync $root | ignore
mut reused = false
for attempt in 0..100 {
	let replacement = tg sandbox create --no-tokens ...$mounts --storage-min 16777216 --storage 16777216 | referent node
	let output = tg run $'--sandbox=($replacement)' --executable /usr/bin/sh -- -c 'dd if=/dev/urandom of=/tmp/data bs=1M count=4' | complete
	tg sandbox destroy $replacement
	tg sandbox wait $replacement | ignore
	if $output.exit_code == 0 {
		$reused = true
		break
	}
	assert ($output.stderr | str contains 'project ID range is exhausted') $output.stderr
	sleep 100ms
}
assert $reused 'the deleted sandbox should release its project ID and storage backing'
server stop $local

# Remove the retained test quota group after its subvolumes have been deleted.
btrfs filesystem sync $root | ignore
btrfs subvolume sync $root | ignore
btrfs qgroup destroy $'1/($project_start)' $root
chmod -R u+rwX $directory
rm -rf $directory
