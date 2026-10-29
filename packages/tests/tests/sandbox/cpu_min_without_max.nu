use ../../lib/test.nu *

# A CPU minimum without a maximum leaves every CPU visible, and a configured default maximum rises to meet a larger minimum.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux CPU limits' }

# Count the CPUs in the effective cpuset of the cgroup parent that holds the sandboxes, which an unlimited sandbox may use.
mut cgroup = container_cgroup_parent [cpu memory pids] | path expand
while not ($cgroup | path join cpuset.cpus.effective | path exists) {
	if $cgroup == '/sys/fs/cgroup' {
		skip_test 'this test requires the cpuset controller in the cgroup hierarchy'
	}
	$cgroup = $cgroup | path dirname
}
let cpus = open --raw ($cgroup | path join cpuset.cpus.effective)
	| str trim
	| split row ','
	| each {|range| let bounds = $range | split row '-' | into int; ($bounds | last) - ($bounds | first) + 1 }
	| math sum
if $cpus < 3 {
	skip_test 'this test requires at least three CPUs'
}

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		let nproc = (name: string) => tg.build`echo ${name} > /dev/null && nproc > ${tg.output}`.env(tg.build(busybox));
		export let unlimited = () => nproc("unlimited");
		export let minimum = () => nproc("minimum").cpu({ min: 1 });
		export let larger = () => nproc("larger").cpu({ min: 2 });
	',
}

def nproc [path: string, export: string] {
	let output = tg build $'($path)#($export)' | complete
	success $output
	tg read ($output.stdout | str trim) | str trim | into int
}

# Without a configured default, an omitted maximum is unlimited.
let local = server spawn --busybox
assert equal (nproc $path unlimited) $cpus 'a sandbox without a CPU maximum should see every CPU'
assert equal (nproc $path minimum) $cpus 'a CPU minimum without a maximum should see every CPU'
server stop $local

# With a configured default maximum, an omitted maximum takes the default, raised to the minimum.
let local = server spawn --busybox --config { scheduler: { default_cpu: 1 } }
assert equal (nproc $path unlimited) 1 'a sandbox without a CPU maximum should take the default maximum'
assert equal (nproc $path minimum) 1 'a CPU minimum within the default maximum should take the default maximum'
assert equal (nproc $path larger) 2 'the default maximum should rise to meet a larger CPU minimum'
