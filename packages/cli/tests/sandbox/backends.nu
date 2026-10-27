use ../lib/test.nu *

# The Linux launch adapters preserve mounts, environment, stdio, and command status.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
let directory = mktemp -d | str trim
let lower = $directory | path join lower
mkdir $lower
let source = $directory | path join input
'input' | save $source
let compiler_source = $directory | path join main.c
'int main(void) { return 0; }' | save $compiler_source
for backend in [builtin bubblewrap] {
    let kind = $backend
    let runtime = if $backend == bubblewrap {
        if (which bwrap | is-empty) { continue }
        [--runtime-path (which bwrap | first | get path)]
    } else { [] }
	let upper = $directory | path join $kind upper
	let work = $directory | path join $kind work
	mkdir $upper $work
	let output = "provided\n" | ^tangram sandbox run --backend $backend ...$runtime --unshare-all --as-pid-1 --uid 0 --gid 0 --overlay-src $lower --overlay $upper $work / ...(program_binds) --ro-bind $source /input --ro-bind $compiler_source /main.c --tmpfs /tmp --dev /dev --proc /proc --clearenv --setenv TEST value --setenv PATH ($env.PATH | str join (char esep)) -- /bin/sh -ec 'read line; test "$line" = provided; test "$TEST" = value; test "$(cat /input)" = input; test -r /proc/self/status; echo null > /dev/null; if (echo invalid > /input) 2>/dev/null; then exit 99; fi; echo output; echo error >&2; echo written > /result; if test -x /usr/bin/cc; then /usr/bin/cc /main.c -o /compiled; /compiled; fi; exit 7' | complete
	assert ($output.exit_code == 7) $"($backend): ($output.stderr)"
	assert equal ($output.stdout | str trim) output
	assert equal ($output.stderr | str trim) error
	assert equal (open --raw ($upper | path join result) | str trim) written
}
