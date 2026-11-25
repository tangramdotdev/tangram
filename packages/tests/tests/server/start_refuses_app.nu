use ../../util/test.nu *

# With a config that declares the Tangram app as the manager, neither `--start` nor `tangram server start` starts a server, `tangram server stop` and `tangram server restart` refuse, and each fails with a hint to use the app.

let directory = mktemp -d
let config_path = $directory | path join 'config.json'
{ manager: app, vfs: false } | to json | save $config_path
let tangram = [--config $config_path --directory ($directory | path join 'server') --no-remotes]

let output = tangram ...$tangram health | complete
failure $output 'the command should fail without a server'
assert ($output.stderr | str contains 'no server is running') $output.stderr
assert ($output.stderr | str contains 'open the Tangram app') $output.stderr

let output = tangram ...$tangram --start health | complete
failure $output '`--start` should not start a server that the app manages'
assert ($output.stderr | str contains 'the server is managed by the Tangram app') $output.stderr
assert ($output.stderr | str contains 'open the Tangram app') $output.stderr

let output = tangram ...$tangram server start | complete
failure $output '`tangram server start` should not start a server that the app manages'
assert ($output.stderr | str contains 'the server is managed by the Tangram app') $output.stderr
assert ($output.stderr | str contains 'open the Tangram app') $output.stderr

let output = tangram ...$tangram server stop | complete
failure $output '`tangram server stop` should refuse for a server that the app manages'
assert ($output.stderr | str contains 'the server is managed by the Tangram app') $output.stderr
assert ($output.stderr | str contains 'use the Tangram app to stop the server') $output.stderr

let output = tangram ...$tangram server restart | complete
failure $output '`tangram server restart` should refuse for a server that the app manages'
assert ($output.stderr | str contains 'the server is managed by the Tangram app') $output.stderr
assert ($output.stderr | str contains 'use the Tangram app to restart the server') $output.stderr

let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json) { manager: app, status: stopped }
