use ../lib/test.nu *

# Every Linux backend keeps podman's default capabilities for a root workload and drops them for other users.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
if (which setfattr | is-empty) { skip_test 'setfattr is not installed' }
let directory = mktemp -d | str trim
for backend in [builtin bubblewrap gvisor] {
	let options = match $backend {
		builtin => [--as-pid-1],
		bubblewrap => {
			if (which bwrap | is-empty) { continue }
			[--runtime-path (which bwrap | first | get path) --as-pid-1]
		},
		gvisor => {
			if (which runsc | is-empty) { continue }
			[--runtime-path (which runsc | first | get path) --platform systrap]
		},
	}
	let root = $directory | path join $backend
	mkdir $root
	let arguments = [--backend $backend ...$options --unshare-all --bind $root / ...(program_binds) --tmpfs /tmp --dev /dev --proc /proc --clearenv --setenv PATH ($env.PATH | str join (char esep))]

	# A root workload keeps the default capabilities, so cp can preserve the xattrs of a read-only file.
	let output = ^tangram sandbox run ...$arguments --uid 0 --gid 0 -- /bin/sh -ec 'grep CapEff /proc/self/status; cd /tmp; echo contents > source; setfattr -n user.test -v value source; chmod 0444 source; cp -a source destination; getfattr --only-values -n user.test destination' | complete
	success $output $"($backend): the root workload should succeed"
	let lines = $output.stdout | lines
	assert equal ($lines | first) "CapEff:\t00000000800405fb" $"($backend): the root workload should keep the default capabilities"
	assert equal ($lines | last) value $"($backend): cp should preserve the xattr"

	# A non-root workload has no capabilities.
	let output = ^tangram sandbox run ...$arguments --uid 1000 --gid 1000 -- /bin/sh -ec 'grep CapEff /proc/self/status' | complete
	success $output $"($backend): the non-root workload should succeed"
	assert equal ($output.stdout | str trim) "CapEff:\t0000000000000000" $"($backend): the non-root workload should have no capabilities"
}
