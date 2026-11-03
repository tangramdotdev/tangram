use ../../util/test.nu *

# Shutting the server down gracefully leaves its sandboxes stopped rather than destroyed.

let local = server spawn

let sandbox = tg sandbox create --no-tokens | referent node

# Interrupt the server as `tangram server stop` does, then start it again.
kill --signal 2 $local.pid
wait_for_exit $local.pid
let local = server start $local

let data = tg sandbox get $sandbox | from json | get data
assert equal $data.status stopped "the sandbox should be stopped after a graceful shutdown"
