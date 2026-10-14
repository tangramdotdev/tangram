use ../lib/test.nu *

# Builtin and bubblewrap agree on PID-1 behavior and the isolation of the reaper's root.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
let directory = mktemp --directory
let secret = $directory | path join secret
'private' | save $secret
let lower = $directory | path join lower
mkdir $lower
for backend in [builtin bubblewrap] {
    if $backend == bubblewrap and (which bwrap | is-empty) { continue }
    for pid1 in [true false] {
        let root = $directory | path join $'($backend)-($pid1)'
        mkdir ($root | path join upper) ($root | path join work)
        let mode = if $pid1 { [--as-pid-1] } else { [] }
        let script = $'printf "%s\n" "$$"; test ! -r "/proc/1/root($secret)"'
        let output = ^tangram sandbox run --backend $backend --unshare-all ...$mode --uid 0 --gid 0 --overlay-src $lower --overlay ($root | path join upper) ($root | path join work) / --ro-bind /usr /usr --ro-bind ('/bin' | path expand) /bin --ro-bind ('/lib' | path expand) /lib --ro-bind ('/lib64' | path expand) /lib64 --proc /proc -- /bin/sh -ec $script | complete
        success $output
        assert equal ($output.stdout | str trim | into int) (if $pid1 { 1 } else { 2 })
    }
}

# A native launcher starts with an empty root unless mounts populate it.
for backend in [builtin bubblewrap] {
    if $backend == bubblewrap and (which bwrap | is-empty) { continue }
    let output = ^tangram sandbox run --backend $backend --unshare-all -- /bin/true | complete
    failure $output
}
