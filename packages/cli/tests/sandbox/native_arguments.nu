use ../lib/test.nu *

# Common launch preparation invokes external backends through their native interfaces.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
let runtime = $env.PWD | path join runtime
'#!/bin/sh
printf "%s\n" "$@"
exit 23
' | save $runtime
^chmod +x $runtime
for backend in [bubblewrap gvisor] {
    let output = ^tangram sandbox run --backend $backend --runtime-path $runtime --unshare-all -- /bin/echo 'value with spaces' -x | complete
    assert equal $output.exit_code 23
    let arguments = $output.stdout | lines
    if $backend == bubblewrap {
        assert ('--bind-fd' in $arguments)
        assert ('--cap-drop' in $arguments)
        assert equal ($arguments | last 3) [/bin/echo 'value with spaces' -x]
    } else {
        assert ('--platform=kvm' in $arguments)
        assert ('--ignore-cgroups=true' in $arguments)
        assert ('--bundle' in $arguments)
    }
}
