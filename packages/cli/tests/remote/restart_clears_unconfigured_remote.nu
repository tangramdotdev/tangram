use ../../test.nu *

# With an empty remotes key in the config, a remote added through the CLI does not survive a restart.

let server = spawn --name plain

tg remote put mine "http://localhost:6666"
assert equal (tg remote list | from json | get name) ["mine"] "the remote should be listed before the restart"

# Restart the server.
let pid = open ($server.directory | path join "lock") | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the server should stop"
spawn --directory $server.directory --name plain --url $server.url

let list = tg remote list | from json
assert ($list | is-empty) "the empty remotes config should clear the added remote"
