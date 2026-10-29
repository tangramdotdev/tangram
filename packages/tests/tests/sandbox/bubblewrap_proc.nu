use ../../util/test.nu *

# The bubblewrap runtime starts with a /proc that matches its PID namespace.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
let runtime = $env.PWD | path join runtime
'#!/bin/sh
read -r pid rest < /proc/self/stat
printf "%s %s\n" "$$" "$pid"
' | save $runtime
^chmod +x $runtime
let output = ^tangram sandbox run --backend bubblewrap --runtime-path $runtime --unshare-all -- /bin/true | complete
success $output
let pids = $output.stdout | str trim | split row ' '
assert equal ($pids | last) ($pids | first) 'the runtime should find its own PID in /proc'
