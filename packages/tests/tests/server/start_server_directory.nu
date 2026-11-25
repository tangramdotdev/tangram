use ../../util/test.nu *

# With a config that sets `server.directory`, `tangram server start` starts a server in that directory, which a client reaches through the matching `client.url` or through the default socket in that directory.

# Remove the inherited URL and token, so that the commands find the server through the config.
hide-env --ignore-errors TANGRAM_URL TANGRAM_TOKEN

let directory = mktemp -d
let server_directory = $directory | path join 'server'
let url = $'http+unix://($server_directory | path join socket | url encode --all)'
let config_path = $directory | path join 'config.json'
{ client: { url: $url }, server: { directory: $server_directory }, vfs: false } | to json | save $config_path
let tangram = [--config $config_path --no-remotes]

let output = tangram ...$tangram server start | complete
success $output 'the server should start'

let output = tangram ...$tangram server status | complete
success $output
let status = $output.stdout | from json
assert equal $status.status started
assert equal $status.manager cli

let output = tangram ...$tangram health | complete
success $output 'a client with the matching url should reach the server'

# A client without a url reaches the server through the socket in the server's directory.
let default_config_path = $directory | path join 'default.json'
{ server: { directory: $server_directory }, vfs: false } | to json | save $default_config_path
let output = tangram --config $default_config_path --no-remotes health | complete
success $output 'a client without a url should reach the server'

let output = tangram ...$tangram server stop | complete
success $output 'the server should stop'

let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json) { status: stopped }
