use ../../util/test.nu *

# Starting a server that fails during startup reports the server's log.

let directory = mktemp --directory
let database_path = $directory | path join 'database'
mkdir $database_path
let config_path = $directory | path join 'config.json'
{ database: { kind: 'sqlite', path: $database_path }, vfs: false } | to json | save $config_path
let tangram = [--config $config_path --directory ($directory | path join 'server') --no-remotes]

let output = tangram ...$tangram server start | complete
failure $output 'the server should fail to start'
assert ($output.stderr | str contains 'the server exited before it was ready') $output.stderr
assert ($output.stderr | str contains 'unable to open database file') $output.stderr
