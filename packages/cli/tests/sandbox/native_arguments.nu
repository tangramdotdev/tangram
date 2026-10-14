use ../lib/test.nu *

# External backends receive their native arguments unchanged.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
let runtime = $env.PWD | path join runtime
'#!/bin/sh
printf "%s\n" "$@"
exit 23
' | save $runtime
^chmod +x $runtime
for backend in [bubblewrap gvisor] {
    let output = ^tangram sandbox run --backend $backend --runtime-path $runtime -- --config native.json --unknown 'value with spaces' -- command -x | complete
    assert equal $output.exit_code 23
    assert equal ($output.stdout | lines) [--config native.json --unknown 'value with spaces' -- command -x]
}

# Tangram setup options are not part of the builtin interface.
for option in [--cgroup --cgroup-fd --filesystem-fd --network-fd --nice --rlimit-nofile --index] {
    let output = ^tangram sandbox run --backend builtin -- $option 0 -- /bin/true | complete
    failure $output
    assert ($output.stderr | str contains $"unexpected argument '($option)'")
}
