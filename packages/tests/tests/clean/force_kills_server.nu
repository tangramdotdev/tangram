use ../../util/test.nu *

# Force cleaning kills a server that `tangram server start` started and that does not exit after SIGINT and SIGTERM, then removes the server's entire directory.

# Remove the inherited URL and token, so that the commands find the server through the config.
hide-env --ignore-errors TANGRAM_URL TANGRAM_TOKEN

let directory = mktemp -d
let server_directory = $directory | path join 'server'
let config_path = $directory | path join 'config.json'
{ server: { directory: $server_directory, kill_timeout: 0.1, stop_timeout: 0.1 }, vfs: false } | to json | save $config_path
let tangram = [--config $config_path --no-remotes]

let server = stubborn_server $server_directory

let output = tangram ...$tangram clean --force | complete
success $output
let output = job recv --timeout 10sec
assert ($output.exit_code != 0) 'the server must have been killed'
assert (not ($server_directory | path exists)) 'the directory should be removed'

let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json) { status: stopped }
