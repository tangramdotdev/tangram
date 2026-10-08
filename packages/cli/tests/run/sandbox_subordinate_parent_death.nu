use ../lib/test.nu *

# A mapped workload dies when its launcher exits, including across a PID namespace.

const driver = path self ../lib/container_parent.py

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
if (which python3 | is-empty) {
	skip_test 'this test requires python3'
}

let parent = container_cgroup_parent []
let maps = container_id_maps
let tangram = which tangram | where type == external | get path | first
let output = ^python3 $driver $tangram ($maps | to json --raw) $parent | complete
success $output 'the mapped workload should die with its launcher'
