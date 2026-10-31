use ../../util/test.nu *

# A sandbox stopped by a graceful shutdown starts again after the server restarts.

let local = server spawn

let sandbox = tg sandbox create --no-tokens | referent node
kill --signal 2 $local.pid
wait_for_exit $local.pid
let local = server start $local

tg sandbox start $sandbox
assert equal (tg sandbox get $sandbox | from json | get data.status) started "the sandbox should start after the restart"
