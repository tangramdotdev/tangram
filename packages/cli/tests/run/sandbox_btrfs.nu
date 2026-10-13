use ../lib/test.nu *

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let root = $env.TANGRAM_TEST_BTRFS_PATH? | default ''
if ($root | is-empty) {
	skip_test 'TANGRAM_TEST_BTRFS_PATH must name a Btrfs filesystem with quotas enabled'
}

let parent = container_cgroup_parent [cpu cpuset memory pids]
let directory = mktemp --directory --tmpdir-path $root
# Quota IDs are filesystem-wide, and other runners may still have pending deletions.
let project_start = (random int 100000..10000000) * 100
let local = server spawn --busybox --directory $directory --config {
	runner: { isolation: { container: { filesystem_project_ids: { start: $project_start, count: 100 } } } },
}
let sandbox = tg sandbox create --no-tokens --storage 16777216 | referent node
let command = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string, sandbox: string) => tg.run`${script}`
			.env(tg.build(busybox))
			.sandbox(sandbox);
	',
}

let output = tg run $command --arg-string 'set -e; test "$(stat -f -c %t /tmp)" = 9123683e; dd if=/dev/urandom of=/tmp/data bs=1M count=4; echo ok' --arg-string $sandbox | complete
success $output 'a sandbox on Btrfs should be able to write below its storage limit'
assert equal ($output.stdout | str trim) 'ok'

let output = tg run $command --arg-string 'dd if=/dev/urandom of=/tmp/full bs=1M count=32' --arg-string $sandbox | complete
failure $output 'a sandbox on Btrfs must not exceed its storage limit'
assert (($output.stderr | str contains 'No space left on device') or ($output.stderr | str contains 'Disk quota exceeded'))

tg sandbox destroy $sandbox
let output = tg sandbox wait $sandbox | complete
success $output 'the Btrfs sandbox should clean up its writable filesystem'
assert equal ($output.stdout | from json) destroyed
let usage = tg sandbox get $sandbox | from json | get data.usage.storage.consumption
assert ($usage > 0) 'the Btrfs sandbox should account for its storage usage'
server stop $local
chmod -R u+rwX $directory
rm -rf $directory
