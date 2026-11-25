use ../../util/test.nu *

# Force cleaning while a server that was run by hand is running fails with the refusal to stop it and leaves the server running.

let local = server spawn

let output = tg -d $local.directory clean --force | complete
failure $output 'force cleaning should fail while a server without a manager is running'
assert ($output.stderr | str contains 'the server was not started by `tangram server start`') $output.stderr

let output = tg health | complete
success $output 'the server should still be running'
