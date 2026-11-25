use ../../util/test.nu *

# A client command fails with a hint to start the server when no server is running, and does not start one.

let directory = mktemp -d
let config_path = $directory | path join 'config.json'
{ vfs: false } | to json | save $config_path
let tangram = [--config $config_path --directory ($directory | path join 'server') --no-remotes]

let output = tangram ...$tangram health | complete
failure $output 'the command should fail without a server'
assert ($output.stderr | str contains 'no server is running') $output.stderr
assert ($output.stderr | str contains '`tangram server start`') $output.stderr
assert ($output.stderr | str contains '`--start`') $output.stderr

let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json) { status: stopped }
