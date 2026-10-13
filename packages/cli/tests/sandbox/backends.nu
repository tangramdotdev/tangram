use ../lib/test.nu *

# The Linux launch adapters preserve mounts, environment, stdio, and command status.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
let directory = mktemp -d | str trim
let lower = $directory | path join lower
mkdir $lower
let bin = '/bin' | path expand
let lib = '/lib' | path expand
let lib64 = '/lib64' | path expand
let source = $directory | path join input
'input' | save $source
let compiler_source = $directory | path join main.c
'int main(void) { return 0; }' | save $compiler_source
for kind in [builtin bubblewrap gvisor-systrap gvisor-kvm] {
	let backend = if ($kind | str starts-with gvisor) { 'gvisor' } else { $kind }
	let runtime = match $backend {
		builtin => [],
		bubblewrap => {
			if (which bwrap | is-empty) { continue }
			[--runtime-path (which bwrap | first | get path)]
		},
		gvisor => {
			if (which runsc | is-empty) { continue }
			let platform = if $kind == gvisor-kvm { 'kvm' } else { 'systrap' }
            if $platform == kvm and ((^test -r /dev/kvm -a -w /dev/kvm | complete).exit_code != 0) { continue }
            [--runtime-path (which runsc | first | get path) --platform $platform]
		},
	}
	let upper = $directory | path join $kind upper
	let work = $directory | path join $kind work
	mkdir $upper $work
	let output = "provided\n" | ^tangram sandbox run --backend $backend ...$runtime --index 0 --unshare-all --as-pid-1 --uid 0 --gid 0 --overlay-src $lower --overlay $upper $work / --ro-bind /usr /usr --ro-bind $bin /bin --ro-bind $lib /lib --ro-bind $lib64 /lib64 --ro-bind $source /input --ro-bind $compiler_source /main.c --tmpfs /tmp --dev /dev --proc /proc --clearenv --setenv TEST value --setenv PATH /usr/bin:/bin -- /bin/sh -ec 'read line; test "$line" = provided; test "$TEST" = value; test "$(cat /input)" = input; test -r /proc/self/status; echo null > /dev/null; if (echo invalid > /input) 2>/dev/null; then exit 99; fi; echo output; echo error >&2; echo written > /result; if test -x /usr/bin/cc; then /usr/bin/cc /main.c -o /compiled; /compiled; fi; exit 7' | complete
	assert ($output.exit_code == 7) $"($backend): ($output.stderr)"
	assert equal ($output.stdout | str trim) output
	assert equal ($output.stderr | str trim) error
	assert equal (open --raw ($upper | path join result) | str trim) written
}
