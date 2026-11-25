use ../../util/test.nu *

# Stopping a server that was run by hand, which has no manager, fails and leaves the server running.

let local = server spawn

let output = tg -d $local.directory server status | complete
success $output
let status = $output.stdout | from json
assert equal $status.status started
assert equal ($status | get --optional manager) null

let output = tg -d $local.directory server stop | complete
failure $output 'stopping a server without a manager should fail'
assert ($output.stderr | str contains 'the server was not started by `tangram server start`') $output.stderr
assert ($output.stderr | str contains '`tangram server run`') $output.stderr

let output = tg health | complete
success $output 'the server should still be running'
