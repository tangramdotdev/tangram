use ../lib/test.nu *

# Hardened containers reject host networking and writable host mounts before creation.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let root = $env.TANGRAM_TEST_PROJECT_QUOTA_PATH? | default ''
if ($root | is-empty) {
	skip_test 'TANGRAM_TEST_PROJECT_QUOTA_PATH is not set'
}
let directory = mktemp --directory --tmpdir-path $root

let cgroup_parent = container_cgroup_parent [cpu memory pids]

let local = server spawn --config {
	directory: $directory,
	runner: {
		isolation: {
			container: ({ harden: true } | merge (container_id_maps)),
		},
	},
}

let output = tg run --sandbox --network=host --executable /bin/true | complete
failure $output 'host networking should be rejected in hardened mode'
assert ($output.stderr | str contains 'host networking is not allowed for hardened container isolation')

let mount = mktemp -d
let output = tg run --sandbox --mount $'($mount):/target,rw' --executable /bin/true | complete
failure $output 'a writable mount should be rejected in hardened mode'
assert ($output.stderr | str contains 'writable mounts are not allowed for hardened container isolation')
