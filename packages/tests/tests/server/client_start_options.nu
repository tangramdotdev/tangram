use ../../util/test.nu *

# The `TANGRAM_START` environment variable and the `client.start` config key start a server, and `--no-start` and `--start=false` override them.

let directory = mktemp -d
let server_directory = $directory | path join 'server'

# Enable start in the config.
let config_path = $directory | path join 'config.json'
{ client: { start: true }, vfs: false } | to json | save $config_path
let tangram = [--config $config_path --directory $server_directory --no-remotes]

let output = tangram ...$tangram --no-start health | complete
failure $output '`--no-start` should override the config'
assert ($output.stderr | str contains 'no server is running') $output.stderr

let output = tangram ...$tangram --start=false health | complete
failure $output '`--start=false` should override the config'
assert ($output.stderr | str contains 'no server is running') $output.stderr

let output = with-env { TANGRAM_START: false } { tangram ...$tangram health | complete }
failure $output 'the environment variable should override the config'
assert ($output.stderr | str contains 'no server is running') $output.stderr

let output = tangram ...$tangram health | complete
success $output 'the config should start the server'
let output = tangram ...$tangram server stop | complete
success $output 'the server should stop'

# Enable start in the environment.
let config_path = $directory | path join 'config_without_start.json'
{ vfs: false } | to json | save $config_path
let tangram = [--config $config_path --directory $server_directory --no-remotes]

let output = with-env { TANGRAM_START: true } { tangram ...$tangram --no-start health | complete }
failure $output '`--no-start` should override the environment variable'
assert ($output.stderr | str contains 'no server is running') $output.stderr

let output = with-env { TANGRAM_START: true } { tangram ...$tangram --start=false health | complete }
failure $output '`--start=false` should override the environment variable'
assert ($output.stderr | str contains 'no server is running') $output.stderr

let output = with-env { TANGRAM_START: true } { tangram ...$tangram health | complete }
success $output 'the environment variable should start the server'
let output = tangram ...$tangram server stop | complete
success $output 'the server should stop'

let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json) { status: stopped }
