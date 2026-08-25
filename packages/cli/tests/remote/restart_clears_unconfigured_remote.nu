use ../../test.nu *

# With an empty remotes key in the config, a remote added through the CLI does not survive a restart.

let server = server spawn --name plain

tg remote put mine "http://localhost:6666"
assert equal (tg remote list | from json | get name) ["mine"] "the remote should be listed before the restart"

# Restart the server.
let server = server restart $server

let list = tg remote list | from json
assert ($list | is-empty) "the empty remotes config should clear the added remote"
